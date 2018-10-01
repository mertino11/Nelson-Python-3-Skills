#  Name:		Mert Cakir
#  Student Number: 	@00518279
#  Course: 		MSc Big Data Tools and Techniques
#  File:		Automation Script PySparkSQL.sh
#  Copyright:		© 2018 Mert Cakir ALL RIGHTS RESERVED

echo "
Name:			Mert Cakir
Student Number: 	@00518279
Course: 		MSc Big Data Tools and Techniques
File:			Automation Script PySparkSQL.sh
Copyright:		© 2018 Mert Cakir ALL RIGHTS RESERVED

_______________________________________________________________________________________

!!! MAKE SURE THAT YOU RUNNED THE SCRIPT PREPARATION.SH OR THIS SCRIPT WON'T WORK !!!

	!!! MAKE SURE THAT THE SCRIPT PySparkSQL.py IS LOCATED IN THE DESKTOP !!!_______________________________________________________________________________________

Script is starting in 5..."
sleep 1

echo "4..."
sleep 1

echo "3..."
sleep 1

echo "2..."
sleep 1

echo "1..."
sleep 1

printf "\nScript is starting NOW! \n\n"

# Runs the Python Script via Terminal.
echo "Starting with Problem Statement 1..."
spark-submit --packages com.databricks:spark-csv_2.10:1.3.0 /home/training/Desktop/PySparkSQLPS1.py

echo "Starting with Problem Statement 2..."
sleep 5
spark-submit --packages com.databricks:spark-csv_2.10:1.3.0 /home/training/Desktop/PySparkSQLPS2.py
echo "CSV-File is created under the location /home/training"

echo "Starting with Problem Statement 3..."
sleep 5
spark-submit --packages com.databricks:spark-csv_2.10:1.3.0 /home/training/Desktop/PySparkSQLPS3.py
sleep 5

printf "\nScript is done!"
printf "\nGoing back to Run.sh..."
/home/training/Desktop/Run.sh
