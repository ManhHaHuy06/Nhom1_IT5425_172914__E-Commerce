@echo off
rem Upload olist-ml\artifacts to MinIO (lakehouse/models/late_risk/<version>/) and point current.json to it.
rem Usage: publish_model.bat <version>      e.g. publish_model.bat v1_T2018-06-01
setlocal EnableDelayedExpansion
set "V=%~1"
if "%V%"=="" goto usage
set "ENVF=%~dp0..\olist-lakehouse\infrastructure\.env"
if not exist "%ENVF%" (echo Missing %ENVF% & exit /b 1)
if not exist "%~dp0artifacts\late_risk.joblib" (echo Run train_model.bat first & exit /b 1)
for /f "usebackq tokens=1,* delims==" %%A in ("%ENVF%") do (
  if /i "%%A"=="MINIO_ROOT_USER" set "MU=%%B"
  if /i "%%A"=="MINIO_ROOT_PASSWORD" set "MP=%%B"
)
set "CJ=%TEMP%\olist_current_json"
if not exist "%CJ%" mkdir "%CJ%"
>"%CJ%\current.json" echo {"version": "%V%"}
docker run --rm --network olist-lakehouse-network -v "%~dp0artifacts:/art:ro" -v "%CJ%:/cur:ro" --entrypoint /bin/sh minio/mc -c "mc alias set l http://minio:9000 !MU! !MP! && mc cp --recursive /art/ l/lakehouse/models/late_risk/%V%/ && mc cp /cur/current.json l/lakehouse/models/late_risk/current.json && mc ls l/lakehouse/models/late_risk/"
exit /b %errorlevel%
:usage
echo Usage: publish_model.bat ^<version^>   e.g. publish_model.bat v1_T2018-06-01
exit /b 1
