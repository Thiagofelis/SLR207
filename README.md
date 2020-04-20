The files for the distributed version are found on folder MapReduce, whereas the ones for the one-machine verion are on folder WordCounter. The textfiles used to test the program are also on the WordCounter folder. Instructions on how to run the code are given below.

For the distributed version (on MapReduce/master):
		java Master filename.txt (1 to see messages, 0 otherwise)

Example:
		java Master ../../WordCounter/domaine_public_fluvial.txt 1

For the one-machine version (WordCounter):
		java WordCounter filename

Please observe that the .txt extension shall not be given in this case!

Example:
		java WordCounter forestier_mayotte
