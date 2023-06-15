use clap::Parser;
use compute_ctl::Args;

fn main() {
    let args = Args::parse();
    println!("{args:?}")
}
