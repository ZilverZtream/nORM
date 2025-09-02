@echo off
echo 🚀 Testing Optimized Joins + Bulk Operations
echo =============================================

cd /d "%~dp0"

echo 🔧 Building nORM with join and bulk optimizations...
cd ..\src
dotnet build -c Release

if %ERRORLEVEL% neq 0 (
    echo ❌ nORM build failed
    exit /b 1
)

echo.
echo 🔧 Building benchmark project...
cd ..\benchmarks
dotnet build -c Release

if %ERRORLEVEL% neq 0 (
    echo ❌ Benchmark build failed
    exit /b 1
)

echo.
echo ✅ Build completed successfully!
echo.
echo 🎯 Testing optimized features...
echo.
echo Key improvements:
echo ✅ Efficient JOIN operations enabled
echo ✅ Optimized SQLite bulk operations
echo ✅ Smart parameter batching
echo ✅ Transaction-based bulk processing
echo.
echo Expected improvements:
echo 📈 Join performance: From 11ns placeholder to competitive with Dapper
echo 📈 Bulk operations: From 8.2M ns (16th) to ~5M ns (top 5)
echo.

dotnet run -c Release -- --fast

echo.
echo 🏆 Performance test completed!
echo.
echo Expected results:
echo ⚡ Query_Join: Should show actual execution time (not 11ns placeholder)
echo ⚡ BulkInsert: Should be significantly faster than 8.2M ns
echo ⚡ Other operations: Should maintain current performance levels
echo.
pause
