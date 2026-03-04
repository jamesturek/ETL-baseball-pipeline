@echo off
cd "C:\Users\james\OneDrive\Desktop\Baseball project\ETL-baseball-pipeline"
"C:\Program Files\R\R-4.5.2\bin\Rscript.exe" main.R >> log.txt 2>&1
exit
