use migraloop_cli::{parse, run};

#[tokio::main]
async fn main() {
    let cli = parse();
    if let Err(err) = run(cli).await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
