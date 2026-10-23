//! `stintmate-ipc-check`: conformance checks for Stintmate IPC v2 endpoints and clients.

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let code = stintmate_ipc_check::cli::main(&argv);
    // The checks leave pipe tasks behind when the peer goes away first: leave at once.
    std::process::exit(code);
}
