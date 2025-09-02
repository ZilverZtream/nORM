#!/bin/bash

echo "🔬 Testing nORM ID Auto-Generation Fix"
echo "======================================"

cd "$(dirname "$0")"

echo "Building project..."
dotnet build -c Release --no-restore

if [ $? -ne 0 ]; then
    echo "❌ Build failed"
    exit 1
fi

echo ""
echo "Testing quick functionality..."
dotnet run -c Release -- --quick

echo ""
echo "Test completed!"
