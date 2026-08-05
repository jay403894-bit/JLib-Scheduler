@echo off
setlocal
cd /d "%~dp0"
call "C:\Program Files\Microsoft Visual Studio\18\Community\VC\Auxiliary\Build\vcvars64.bat" >nul

echo Building bench.exe (release, against deployed C:\libs\Scheduler)...
cl /nologo /std:c++20 /EHsc /MD /O2 /DNDEBUG /I C:\libs\Scheduler\include bench.cpp ^
   /link /LIBPATH:C:\libs\Scheduler\lib\release Scheduler.lib Synchronization.lib
if errorlevel 1 (
    echo BUILD FAILED
    exit /b 1
)
echo OK: bench.exe

