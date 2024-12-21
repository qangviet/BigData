@echo off
:: Lưu tham số vào biến
set cmd=%1
set configPath=%2

:: Hàm hướng dẫn sử dụng
:usage
echo Usage: run.bat ^<command^> ^<arguments^>
echo Available commands:
echo  register_connector          Register a new Kafka connector
echo Available arguments:
echo  [connector config path]     Path to connector config, for command register_connector only
goto :eof

:: Kiểm tra lệnh có được cung cấp hay không
if "%cmd%"=="" (
    echo Missing command
    call :usage
    exit /b 1
)

:: Xử lý lệnh register_connector
if "%cmd%"=="register_connector" (
    if "%configPath%"=="" (
        echo Missing connector config path
        call :usage
        exit /b 1
    ) else (
        echo Registering a new connector from %configPath%
        :: Sử dụng curl để gửi yêu cầu HTTP
        curl -i -X POST -H "Accept:application/json" -H "Content-Type: application/json" http://localhost:8083/connectors -d @%configPath%
        if errorlevel 1 (
            echo Failed to register connector
            exit /b 1
        ) else (
            echo Connector registered successfully
        )
    )
    goto :eof
)

:: Lệnh không hợp lệ
echo Unknown command: %cmd%
call :usage
exit /b 1
