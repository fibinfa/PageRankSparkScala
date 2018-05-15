
1) PageRank :
Files:
Source Code : src

Main method inside Object: PageRank.scala
Parser -> Bz2WikiParser.java


Execution using Eclipse

Steps to run 

   1. Create a maven project.
   2. Create input directory and download wikipedia-simple-html.bz2 
   4. Update the pom.xml files with dependencies and rebuild the maven project.
   5. Go to Run Configuration and provide the input file path and output filepath as argument.
   6. Run the program
   7. Check the output in the output directory
_________________________________________________________________________________________________________________________________


2)Page Rank on AWS Execution

Steps to Run PageRank Program in AWS
   1. Create s3 bucket and upload input files
   2. Mention the number of nodes required as 6 for first run and 11 for second.
   3. Give the main class name and subnet id correctly
   4. Run make cloud command
   5. Check the output in the output directory mentioned

Logs of AWS execution is provided inside AWS folder
	 AWS -> 6M  for six machine run
	 AWS -> 11M for 11 machine run

Output is given in AWS -> Output folder
_________________________________________________________________________________________________________________________________
