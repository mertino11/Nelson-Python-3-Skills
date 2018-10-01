#  Name:		Mert Cakir
#  Student Number: 	@00518279
#  Course: 		MSc Big Data Tools and Techniques
#  File:		List to run scripts.
#  Copyright:		© 2018 Mert Cakir ALL RIGHTS RESERVED

echo "
Name:			Mert Cakir
Student Number: 	@00518279
Course: 		MSc Big Data Tools and Techniques
File:			List to run scripts.
Copyright:		© 2018 Mert Cakir ALL RIGHTS RESERVED

_______________________________________________________________________________________

			Hello and welcome to Mert's Run All script.


Which script would you like to run? (enter only numbers ('1', '2', '3' or '4')
_______________________________________________________________________________________

Option	| Filename     | Description
_______________________________________________________________________________________

1.	Preparation.sh | Loads MySQL, HDFS and Impala data.
2. 	Impala.sh      | Starts answering Problem Statements in Impala + bonus marks.
3. 	PySparkSQL.sh  | Starts answering some of the Problem Statements in PySpark SQL.
4.	PySpark2.0.sh  | Starts answering Problem Statements in PySpark 2.0+.
5.	AllinOne.sh    | Runs all scripts back-to-back.

NOTE: Please make sure that all the data that you have downloaded are in the Desktop.
_______________________________________________________________________________________
"
# Reads user input.
printf "Choose option: "
read userinput

# If elif statement to decide what script(s) the user wants to run.
if [ $userinput == 1 ]
then
	printf "\n You chose option 1: Preparation.sh."
	printf "\n Starting now!\n"
	/home/training/Desktop/Preparation.sh
	printf "\nScript is done! \n"

elif [ $userinput == 2 ]
then
	printf "\n You chose option 2: Impala.sh."
	printf "\n Starting now!\n"
	/home/training/Desktop/Impala.sh
	printf "\nScript is done! \n"

elif [ $userinput == 3 ]
then
	printf "\n You chose option 3: PySparkSQL.sh." 
	printf "\n Starting now!\n"
	/home/training/Desktop/PySparkSQL.sh
	printf "\nScript is done! \n"

elif [ $userinput == 4 ]
then
	printf "\n You chose option 4: PySpark2.0.sh."
	printf "\n Starting now!\n"
	/home/training/Desktop/PySpark2.0.sh
	printf "\nScript is done! \n"

elif [ $userinput == 5 ]
then
	printf "\n You chose option 5: AllinOne.sh."
	printf "\n Starting now!\n"
	/home/training/Desktop/Preparation.sh
	/home/training/Desktop/Impala.sh
	/home/training/Desktop/PySparkSQL.sh
	/home/training/Desktop/PySpark2.0.sh
	printf "\nScript is done! \n"

else
	printf "\nInvalid input. Exiting now...\n"
fi




