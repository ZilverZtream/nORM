#!/bin/bash

echo "========================================"
echo "       nORM Performance Benchmarks"
echo "========================================"
echo

cd "$(dirname "$0")"

if [ "$1" = "--quick" ]; then
    echo "Running quick functionality tests..."
    echo
    dotnet run -c Release -- --quick
    exit 0
fi

if [ "$1" = "--help" ]; then
    echo "Usage:"
    echo "  ./run-benchmarks.sh          Run full benchmark suite"
    echo "  ./run-benchmarks.sh --quick  Run quick functionality test"
    echo "  ./run-benchmarks.sh --help   Show this help"
    echo
    echo "Full benchmarks take 10-15 minutes to complete."
    echo "Quick tests take 30 seconds and verify functionality."
    exit 0
fi

echo "This will run comprehensive performance benchmarks comparing:"
echo "  - nORM"
echo "  - Entity Framework Core"
echo "  - Dapper"
echo "  - Raw ADO.NET"
echo
echo "The benchmark will take 10-15 minutes to complete."
echo "Results will be saved in BenchmarkDotNet.Artifacts/results/"
echo
echo "Press Enter to continue, or Ctrl+C to cancel..."
read

echo
echo "Starting benchmarks..."
dotnet run -c Release

echo
echo "Press Enter to exit..."
read
