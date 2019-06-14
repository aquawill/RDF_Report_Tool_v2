@echo off
SETX CLASSPATH " .;%cd%\hsqldb.jar;%cd%\ojdbc7.jar;%cd%\tools.jar"
rdf_tool_v2\rdf_tool_v2.exe