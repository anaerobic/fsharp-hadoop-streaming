cd Classes

C:\Apps\java\openjdk7\bin\jar -cfvm ..\hadoop-streaming-ms.jar Manifest.txt com\microsoft\hadoop\mapreduce\lib\input\*.class org\apache\hadoop\streaming\*.class org\apache\mahout\classifier\bayes\*.class

cd ..