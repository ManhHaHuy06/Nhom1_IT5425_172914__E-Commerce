@echo off
rem Train the ML weights INSIDE the Spark container, so the pickle is written with the same scikit-learn that the predict stage reads it with.
rem Usage: train_model.bat [T]     T = first day NOT used for training (default 2018-06-01)
set "T=%~1"
if "%T%"=="" set "T=2018-06-01"
docker exec olist-spark-master python3 -m olist_ml.train --data-dir /data --out /opt/olist-ml/artifacts --train-end %T%
if errorlevel 1 exit /b 1
echo.
echo Done. The weights are in olist-ml\artifacts. Next: publish_model.bat v1_T%T%
