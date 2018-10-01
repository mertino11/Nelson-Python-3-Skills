#  Student Number: 	@00518279
#  Course: 		MSc Big Data Tools and Techniques
#  File:		Automation Script PySpark Version 2.0+
#  Copyright:		© 2018 Mert Cakir ALL RIGHTS RESERVED

echo "
Name:			Mert Cakir
Student Number: 	@00518279
Course: 		MSc Big Data Tools and Techniques
File:			Automation Script PySpark Version 2.0+
Copyright:		© 2018 Mert Cakir ALL RIGHTS RESERVED

_______________________________________________________________________________________

!!! MAKE SURE THAT YOU RUNNED THE SCRIPT PREPARATION.SH OR THIS SCRIPT WON'T WORK !!!

	!!! MAKE SURE THAT THE SCRIPT PySpark2.0.py IS LOCATED IN THE DESKTOP !!!_______________________________________________________________________________________

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
spark-submit --packages com.databricks:spark-csv_2.10:1.3.0 /home/training/Desktop/PySpark2.0.py

printf "\nScript is done!"
printf "\nGoing back to Run.sh..."
/home/training/Desktop/Run.sh
