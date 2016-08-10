#!/usr/bin/env python
import sys, os

# python parse_csv.py dir
	
def convert_csv(filename):
	f = open(filename, 'r')
	path, filen = os.path.split(filename)
	filebase, ext = os.path.splitext(filen)
	print os.path.join(path,filebase+".csv")
	new_f = open(os.path.join(path,filebase+".csv"), 'w+')
	num_col = 0
	for line in f:
		row = line.split('\t')
		row = [x.strip() for x in row]
		if num_col is 0:
			num_col = len(row)
			new_f.write(','.join(row)+"\n")
		else:
			if len(row) is num_col:
				q_row = ["\""+x+"\"" for x in row]
				new_f.write(','.join(q_row)+"\n")
			else:
				print "Error - len(row) should be " + str(num_col) + " but is found as " + str(len(row)) + ". Ignoring row."
	new_f.close()
	
	
def main():
	args = sys.argv
	f = args[1]
	files = os.listdir(f)
	for file in files:
		filen, ext = os.path.splitext(file)
		if "txt" in ext:
			convert_csv(os.path.join(f,file))

if __name__ == "__main__":
    main()