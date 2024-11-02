how to start your spark cluster:
    1. make sure dfs and yarn cluster are running
    2. run 'start-master.sh' to start the spark cluster
    3. run 'start-workers.sh' to start the spark workers
    4. if you want to open the spark shell on the terminal, run 'spark-shell'

How to compile the code and create the jar:
    (Run this in the same directory as your .pom file and the jar will appear in the target directory): 'mvn clean package'
How to run the jar on the cluster:
    (cd into DataProcessing/target): 'spark-submit --class org.processing.FlightDataProcessing --master spark://bogota.cs.colostate.edu:30296 FlightDataProcessing-1.0-SNAPSHOT.jar /435_TP/flight_data'

HOW TO TAR:
    tar -cvf <your_userid>.tar ~/FolderName

Datasets
    csv files for flight data from 2018-2020