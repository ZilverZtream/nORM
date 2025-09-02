#!/bin/bash

echo "🔧 Rebuilding nORM with Performance Fix"
echo "======================================="

cd "$(dirname "$0")"

echo "Building nORM core library..."
cd ../src
dotnet build -c Release

if [ $? -ne 0 ]; then
    echo "❌ nORM build failed"
    exit 1
fi

echo ""
echo "Building benchmark project..."
cd ../benchmarks
dotnet build -c Release

if [ $? -ne 0 ]; then
    echo "❌ Benchmark build failed"
    exit 1
fi

echo ""
echo "✅ Build completed successfully!"
echo ""
echo "🚀 Testing performance fix..."
dotnet run -c Release -- --fast

echo ""
echo "🎯 Performance test completed!"
