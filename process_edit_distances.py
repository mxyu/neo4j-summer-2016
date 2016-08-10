#!/usr/bin/env python

from py2neo import authenticate, Graph
from py2neo import Node, Relationship
import difflib
import csv

authenticate("localhost:7474", "neo4j", "neo3j")
# default uri for local Neo4j instance
graphdb = Graph('http://localhost:7474/db/data')


def connect_people_to_concepts():
	people = graphdb.run("MATCH (p:Person) RETURN p.person_id").data()
	for person in people:
		graphdb.run("match (p:Person {person_id: \""+person['p.person_id']+"\"})--(c:ConditionOccurrence)--(con:Concept) WITH p.person_id as person, con.concept_id as concepts, min(c.condition_start_date) as start_date  ORDER BY  start_date, concepts MATCH (p1:Person {person_id: person}) MATCH (c2:Concept {concept_id: concepts}) MERGE (p1)-[:DIAGNOSED_WITH {start_date: start_date}]->(c2)")


def set_NEXT_rels(person_id):
	concepts = graphdb.run("MATCH (p:Person {person_id: \""+person_id+"\"})-[d:DIAGNOSED_WITH]->(c:Concept) RETURN c.concept_id, d.start_date ORDER BY d.start_date, c.concept_id").data()
	for i in range(0, len(concepts)-1):
		d = graphdb.run("MATCH (p:Person {person_id: \""+person_id+"\"})-[d1:DIAGNOSED_WITH {start_date: \""+concepts[i]['d.start_date']+"\"}]->(c1:Concept {concept_id: \""+concepts[i]['c.concept_id']+"\"})  MATCH (p:Person {person_id: \""+person_id+"\"})-[d2:DIAGNOSED_WITH {start_date: \""+concepts[i+1]['d.start_date']+"\"}]->(c2:Concept {concept_id: \""+concepts[i+1]['c.concept_id']+"\"}) MERGE (c1)-[:NEXT {person_id: \""+person_id+"\", start_date_d1: d1.start_date, start_date_d2: d2.start_date}]->(c2)").data()
		
	#concepts = graphdb.run("MATCH (c1:Concept)-[n:NEXT {person_id: \""+person_id+"\"}]->(c2:Concept) RETURN c1.concept_id, n.start_date1, n.start_date2, c2.concept_id").data()

def process_conditions_by_person():
	people = graphdb.run("MATCH (p:Person) RETURN p.person_id").data()
	for person in people:
		set_NEXT_rels(person['p.person_id'])

		
# Extract path as list 
# see comment for extract_all_concept_paths_by_person()
def extract_single_concept_path(person_id):
	first = graphdb.run("match (p:Person {person_id: \""+person_id+"\"})-[d:DIAGNOSED_WITH]->(c:Concept) return c.concept_id, min(d.start_date) AS date ORDER BY date, c.concept_id LIMIT 1").data()
	p = graphdb.run("match (p:Person {person_id: \""+person_id+"\"})-[:DIAGNOSED_WITH]->(c:Concept)-[n:NEXT {person_id: \""+person_id+"\"}]->(c2) return c.concept_id, c2.concept_id ORDER BY n.start_date_d1, c.concept_id").data()
	#n.start_date_d1, n.start_date_d2 
	first_node = first[0]['c.concept_id']
	path = [first_node]
	for next_rel in p:
		next = next_rel['c2.concept_id']
		path.append(next)		
	#print str(person_id)+ "\t" + str(len(path))
	return path

# Extract paths as a dictionary
# { 'person_id#': [concept_id1, concept_id2, ...]}
def extract_all_concept_paths_by_person():
	people = graphdb.run("MATCH (p:Person) RETURN p.person_id").data()
	paths = {}
	for person in people:
		paths[str(person['p.person_id'])] = extract_single_concept_path(person['p.person_id'])
	return paths
	
	
# Input = two arrays. This function is pretty useless.
def find_similarity_ratio_between_two_paths(path1, path2):
	s = difflib.SequenceMatcher(path1, path2)
	return s.ratio()

# Input = list of nodes, dictionary of paths by person_id	
# uses ratio() from difflib
def find_similarity_ratio_one_to_all_paths(path, paths, limit):
	ratios = []
	for person, path2 in paths.items():
		s = difflib.SequenceMatcher(None, path, path2)
		ratios.append([person, s.ratio(), path2])
	sorted_ratios = sorted(ratios, key=lambda tup: tup[1], reverse=True)
	for x in sorted_ratios[0:limit-1]:
		print "Person: ", x[0]
		print "Ratio: ", x[1]
		print "\n"
	print "Neo4j query for visualization sent to file neo4j_query.txt "
	ids = ",".join(("\""+str(x)+"\"" for x in (y[0] for y in sorted_ratios[0:limit-1])))
	f = open('neo4j_query.txt', 'w+')
	f.write( "match (p:Person)-[d:DIAGNOSED_WITH]->(c:Concept)-[n:NEXT]->(c2:Concept) WHERE p.person_id IN ["+ids+"] AND n.person_id IN ["+ids+"] return p, d, c, c2, n" )
	f.close()

# Calculates similarity ratios between all paths and outputs two files:
#    1) 'similarity_indices_all_by_all.csv' - matrix format
#    2) 'sorted_similarity_indices.txt' - sorted list of pairs and their ratios
def find_similarity_ratio_all_to_all(paths):
	person_id_list = []
	paths_list = []
	for person, path in paths.items():
		person_id_list.append(person)
		paths_list.append(path)
	ratios_matrix = []
	a = []
	[ratios_matrix.append([a.append(None) for i in range(0, len(person_id_list))]) for i in range(0, len(person_id_list))]
	for i in range(0, len(paths_list)):
		for j in range(i,len(paths_list)):
			s = difflib.SequenceMatcher(None, paths_list[i], paths_list[j])
			ratios_matrix[i][j] = s.ratio()
			
	f = open('similarity_indices_all_by_all.csv', 'w+')
	f.write(','+','.join(person_id_list) + '\n')
	for i in range(0, len(person_id_list)):
		f.write(person_id_list[i]+',')
		f.write(','.join((str(x) for x in ratios_matrix[i])))
		f.write('\n')
	f.close()
	f2 = open('sorted_similarity_indices.csv', 'w+')
	list_form = []
	for i in range(0,len(person_id_list)):
		for j in range(i, len(person_id_list)):
			if i != j:
				list_form.append([person_id_list[i], person_id_list[j], ratios_matrix[i][j], paths_list[i], paths_list[j]])
	sorted_list = sorted(list_form, key=lambda tup: tup[2], reverse=True)
	f2.write('person_id_1, person_id_2, similarity_ratio, path_1, path_2\n')
	for i in range(0,1000):
		row = sorted_list[i]
		f2.write(','.join((str(x) for x in row))+'\n')
	f2.close()
	return ratios_matrix, sorted_list

def count_most_common_similar_paths(sorted_file, ratio_threshold):
	counts = []
	with open(sorted_file, 'rb') as f:
		reader = csv.reader(f)
		header = reader.next()
		for row in csv:
			if row[2] > ratio_threshold:
				a = 0
	
def get_person_path_names_table(paths):
	f = open('person_path_concept_names.csv', 'w+')
	f.write('person_id, path')
	for person, path in paths.items():
		f.write('\n'+person)
		for concept in path:
			name = graphdb.run("MATCH (c:Concept {concept_id: \""+concept+"\"}) return c.concept_name").data()
			f.write(','+name[0]['c.concept_name'])
	f.close()
	
def get_person_path_ids_table(paths):
	f = open('person_path_concept_ids.csv', 'w+')
	f.write('person_id, path')
	for person, path in paths.items():
		f.write('\n'+person)
		for concept in path:
			f.write(','+concept)
	f.close()
	
def main():
	setup = True
	if setup:
		print "Connecting people to concepts..."
		connect_people_to_concepts()
		print "Connecting concepts by date of diagnosis..."
		process_conditions_by_person()
	#extract_single_concept_path("2851389")
	print "Extracting all diagnosis paths ..."
	paths_by_person = extract_all_concept_paths_by_person()
	print "Finding similarity ratios ..."
	#find_similarity_ratio_one_to_all_paths(paths_by_person['6194114'], paths_by_person, 10)
	find_similarity_ratio_all_to_all(paths_by_person)
	print "Creating table of concept names in paths by person_id..."
	get_person_path_names_table(paths_by_person)
	print "Creating table of concept IDs in paths by person_id..."
	get_person_path_ids_table(paths_by_person)
	#count_most_common_similar_paths('sorted_similarity_indices.csv',0.6)
	
if __name__ == "__main__":
	main()