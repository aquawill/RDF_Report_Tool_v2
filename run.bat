@echo off
SETX CLASSPATH " .;%cd%\hsqldb.jar;%cd%\ojdbc7.jar;%cd%\tools.jar"
.\venv\Scripts\python.exe .\rdf_tool_v2.py