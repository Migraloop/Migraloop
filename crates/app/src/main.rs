use migraloop_cli::{parse, run};

#[tokio::main]
async fn main() {
    // Keep modular monorepo seams in the single v1 binary dependency graph.
    let _seams = [
        migraloop_capture::SEAM,
        migraloop_transform::SEAM,
        migraloop_delivery::SEAM,
        migraloop_runtime::SEAM,
    ];
    debug_assert_eq!(_seams.len(), 4);

    let cli = parse();
    if let Err(err) = run(cli).await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
