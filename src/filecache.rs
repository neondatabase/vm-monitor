// TODO: should all fields be pub(crate)?

use anyhow::{bail, Context, Result};
use tokio_postgres::Client;
use tokio_postgres::NoTls;
use tracing::info;
use tracing::warn;

#[derive(Debug)]
pub struct FileCacheState {
    client: Client,
    pub(crate) config: FileCacheConfig,
}

#[derive(Debug)]
pub struct FileCacheConfig {
    /// InMemory indicates whether the file cache is *actually* stored in memory (e.g. by writing to
    /// a tmpfs or shmem file). If true, the size of the file cache will be counted against the
    /// memory available for the cgroup.
    pub(crate) in_memory: bool,

    // ResourceMultiplier gives the size of the file cache, in terms of the size of the resource it
    // consumes (currently: only memory)
    //
    // For example, setting ResourceMultiplier = 0.75 gives the cache a target size of 75% of total
    // resources.
    //
    // This value must be strictly between 0 and 1.
    resource_multiplier: f64,

    // MinRemainingAfterCache gives the required minimum amount of memory, in bytes, that must
    // remain available after subtracting the file cache.
    //
    // This value must be non-zero.
    min_remaining_after_cache: u64,

    // SpreadFactor controls the rate of increase in the file cache's size as it grows from zero
    // (when total resources equals MinRemainingAfterCache) to the desired size based on
    // ResourceMultiplier.
    //
    // A SpreadFactor of zero means that all additional resources will go to the cache until it
    // reaches the desired size. Setting SpreadFactor to N roughly means "for every 1 byte added to
    // the cache's size, N bytes are reserved for the rest of the system, until the cache gets to
    // its desired size".
    //
    // This value must be >= 0, and must retain an increase that is more than what would be given by
    // ResourceMultiplier. For example, setting ResourceMultiplier = 0.75 but SpreadFactor = 1 would
    // be invalid, because SpreadFactor would induce only 50% usage - never reaching the 75% as
    // desired by ResourceMultiplier.
    //
    // SpreadFactor is too large if (SpreadFactor+1) * ResourceMultiplier is >= 1.
    spread_factor: f64,
}

impl Default for FileCacheConfig {
    fn default() -> Self {
        Self {
            in_memory: true,
            resource_multiplier: 0.75,                  // 75 %
            min_remaining_after_cache: 640 * (1 << 20), // 640 MiB; (512 + 128)
            spread_factor: 0.1, // ensure any increase in file cache size is split 90-10 with 10% to other memory
        }
    }
}

impl FileCacheConfig {
    pub fn validate(&self) -> Result<()> {
        // Single field validity
        if !(0.0 < self.resource_multiplier && self.resource_multiplier < 1.0) {
            bail!(
                "resource_multiplier must be between 0.0 and 1.0 exclusive, got {}",
                self.resource_multiplier
            )
        } else if self.spread_factor < 0.0 {
            // TODO: does floating point stuff require we check !(self.spread_factor >= 0.0)?
            bail!("spread_factor must be >= 0, got {}", self.spread_factor)
        } else if self.min_remaining_after_cache == 0 {
            bail!("min_remaining_after_cache must not be 0");
        }

        // Check that ResourceMultiplier and SpreadFactor are valid w.r.t. each other.
        //
        // As shown in CalculateCacheSize, we have two lines resulting from ResourceMultiplier and
        // SpreadFactor, respectively. They are:
        //
        //                        total                                 MinRemainingAfterCache
        //   size = —————————————————— - ————————————————————————
        //                  SpreadFactor + 1                               SpreadFactor + 1
        //
        // and
        //
        //   size = ResourceMultiplier × total
        //
        // .. where 'total' is the total resources. These are isomorphic to the typical 'y = mx + b'
        // form, with y = "size" and x = "total".
        //
        // These lines intersect at:
        //
        //               MinRemainingAfterCache
        //   —————————————————————————————————————————————
        //    1 - ResourceMultiplier × (SpreadFactor + 1)
        //
        // We want to ensure that this value (a) exists, and (b) is >= MinRemainingAfterCache. This is
        // guaranteed when 'ResourceMultiplier × (SpreadFactor + 1)' is less than 1.
        // (We also need it to be >= 0, but that's already guaranteed.)

        let intersect_factor = self.resource_multiplier * (self.spread_factor + 1.0);
        if intersect_factor >= 1.0 {
            // TODO: once again, does floating point evilness require we check if
            // !(intersect_factor < 1.0)
            bail!("incompatible ResourceMultiplier and SpreadFactor");
        }
        Ok(())
    }

    /// Calculate the desired size of the cache, given the total memory
    pub fn calculate_cache_size(&self, total: u64) -> u64 {
        let available = total.saturating_sub(self.min_remaining_after_cache);
        if available == 0 {
            return 0;
        }

        // Conversions to ensure we don't overflow from floating-point ops
        let size_from_spread =
            0i64.max((available as f64 / (1.0 + self.spread_factor)) as i64) as u64;

        let size_from_normal = (total as f64 * self.resource_multiplier) as u64;

        let byte_size = size_from_spread.min(size_from_normal);

        let mib: u64 = 1 << 20;

        // The file cache operates in units of mebibytes, so the sizes we produce should
        // be rounded to a mebibyte. We wound down to be conservative.
        byte_size / mib * mib
    }
}

impl FileCacheState {
    pub async fn new(conn_str: &str, config: FileCacheConfig) -> Result<Self> {
        let (client, conn) = tokio_postgres::connect(conn_str, NoTls)
            .await
            .context("Failed to connect to pg client")?;

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                warn!("postgres error: {e}")
            }
        });

        config.validate().context("File cache config is invalid")?;
        Ok(Self { client, config })
    }

    pub async fn get_file_cache_size(&self) -> Result<u64> {
        Ok(self
            .client
            .query_one(
                "SELECT pg_size_bytes(current_setting('neon.file_cache_size_limit'));",
                &[],
            )
            .await
            .context("Failed to query pg for file cache size")?
            // pg_size_bytes returns a bigint which is the same as an i64.
            .try_get::<_, i64>(0)
            // Since the size of the table is not negative, the cast is sound.
            .map(|bytes| bytes as u64)
            .context("Failed to extract file cache size from query result")?)
    }

    pub async fn set_file_cache_size(&self, num_bytes: u64) -> Result<u64> {
        let max_bytes = self
            .client
            .query_one(
                "SELECT pg_size_bytes(current_setting('neon.max_file_cache_size'));",
                &[],
            )
            .await
            .context("Failed to query pg for max file cache size")?
            .try_get::<_, i64>(0)
            .map(|bytes| bytes as u64)
            .context("Failed to extract amx file cache size form query result")?;

        let mut num_mb = num_bytes / (1 << 20);
        let max_mb = max_bytes / (1 << 20);

        if num_bytes > max_bytes {
            num_mb = max_mb;
        }

        let capped = if num_bytes > max_bytes {
            " (capped) by maximum size"
        } else {
            ""
        };

        info!("Updating file cache size to {num_mb}MiB{capped}, max size = {max_mb}",);

        self.client
            .execute(
                &format!("ALTER SYSTEM SET neon.file_cache_size_limit = {};", num_mb),
                &[],
            )
            .await
            .context("Failed to change file cache size limit")?;

        self.client
            .execute("SELECT pg_reload_conf();", &[])
            .await
            .context("Failed to reload config")?;

        Ok(num_mb * (1 << 20))
    }
}
