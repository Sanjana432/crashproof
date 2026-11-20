import argparse
import asyncio
import os


def main():
    parser = argparse.ArgumentParser(description="CrashProof local workflow laboratory")
    sub = parser.add_subparsers(dest="command", required=True)
    server = sub.add_parser("server", help="Run the local API and dashboard")
    server.add_argument("--port", type=int, default=int(os.getenv("CRASHPROOF_PORT", "8101")))
    worker = sub.add_parser("worker", help="Run a Temporal worker")
    worker.add_argument("--allow-crash", action="store_true", help="Allow the dedicated worker to intentionally exit at a committed payment boundary")
    demo = sub.add_parser("demo", help="Run a genuine worker crash/restart demonstration against Temporal")
    demo.add_argument("--output", default="data/demo-result.json")
    benchmark = sub.add_parser("benchmark", help="Measure real workflow completion latency")
    benchmark.add_argument("--orders", type=int, default=30)
    benchmark.add_argument("--concurrency", type=int, default=5)
    benchmark.add_argument("--output", default="benchmark-results/latest.json")
    args = parser.parse_args()
    if args.command == "server":
        import uvicorn
        uvicorn.run("crashproof.api:create_app", factory=True, host="127.0.0.1", port=args.port)
    elif args.command == "worker":
        from .worker import run_worker
        asyncio.run(run_worker(args.allow_crash))
    elif args.command == "demo":
        from .demo import run_demo
        asyncio.run(run_demo(args.output))
    else:
        from .benchmark import run_benchmark
        asyncio.run(run_benchmark(args.orders, args.concurrency, args.output))


if __name__ == "__main__":
    main()
