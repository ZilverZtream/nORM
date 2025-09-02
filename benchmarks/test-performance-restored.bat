@echo off
echo 🚀 Testing nORM Performance Restoration
echo ========================================

cd /d "%~dp0"

echo Building nORM core library...
cd ..\src
dotnet build -c Release

if %ERRORLEVEL% neq 0 (
    echo ❌ nORM build failed
    exit /b 1
)

echo.
echo Building benchmark project...
cd ..\benchmarks
dotnet build -c Release

if %ERRORLEVEL% neq 0 (
    echo ❌ Benchmark build failed
    exit /b 1
)

echo.
echo ✅ Build completed successfully!
echo.
echo 🎯 Running performance test...
echo Expected: Query ~39ns, Count ~24ns (not 100,000ns!)
echo.

dotnet run -c Release -- --fast

echo.
echo 🏁 Performance test completed!
echo If you see ~39ns for queries, performance is RESTORED! 🎉
echo If you see 100,000ns+, we still have an issue... 😅
pause
