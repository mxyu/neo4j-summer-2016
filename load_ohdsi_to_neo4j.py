#!/usr/bin/env python

from py2neo import authenticate, Graph
from py2neo import Node, Relationship
import difflib
import csv

authenticate("localhost:7474", "neo4j", "neo3j")
# default uri for local Neo4j instance
graphdb = Graph('http://localhost:7474/db/data')

def write_to_file(s):
	f = open("neo4jquery.txt", "w+")
	f.write(s)
	f.close()

def load_csv_to_nodes(filename, node_type):
	header = []
	with open(filename, 'rb') as csvfile:
		reader = csv.reader(csvfile)
		header = reader.next()
		header = [x.decode('utf-8-sig').strip() for x in header]
	csvfile.close()
	db_insert_tmp = [item + ": trim(row." + item +")" for item in header]
	db_insert_str = ', '.join(db_insert_tmp)
	s = "USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM \"file:///c:/diabetes_icd9_related_conditions/"+filename+"\" AS row CREATE (:"+node_type+" {" + db_insert_str + "});"
	graphdb.run(s)
	return s
	
	
def load_csv_to_relationships(filename, n1, n2, id1, rid1, id2, rid2, rel_name):
	s = "USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM \"file:///c:/diabetes_icd9_related_conditions/"+filename+"\" AS row MATCH (n1:"+n1+" {"+id1+": trim(row."+rid1+")}) MATCH (n2:"+n2+" {"+id2+": trim(row."+rid2+")}) MERGE (n1)-[:"+rel_name+"]->(n2);"
	graphdb.run(s)
	return s

	
	
def connect_people_to_concepts():
	people = graphdb.run("MATCH (p:Person) RETURN p.person_id").data()
	for person in people:
		s = "match (p:Person {person_id: \""+person['p.person_id']+"\"})--(c:ConditionOccurrence)--(con:ICD9) WITH p.person_id as person, con.I9 as concepts, min(c.condition_start_date) as start_date  ORDER BY  start_date, concepts MATCH (p1:Person {person_id: person}) MATCH (c2:ICD9 {I9: concepts}) MERGE (p1)-[:DIAGNOSED_WITH {start_date: start_date}]->(c2)"
		graphdb.run(s)
		return s


def set_NEXT_rels(person_id):
	concepts = graphdb.run("MATCH (p:Person {person_id: \""+person_id+"\"})-[d:DIAGNOSED_WITH]->(c:ICD9) RETURN c.I9, d.start_date ORDER BY d.start_date, c.I9").data()
	for i in range(0, len(concepts)-1):
		s = "MATCH (p:Person {person_id: \""+person_id+"\"})-[d1:DIAGNOSED_WITH {start_date: \""+concepts[i]['d.start_date']+"\"}]->(c1:ICD9 {I9: \""+concepts[i]['c.I9']+"\"})  MATCH (p:Person {person_id: \""+person_id+"\"})-[d2:DIAGNOSED_WITH {start_date: \""+concepts[i+1]['d.start_date']+"\"}]->(c2:ICD9 {I9: \""+concepts[i+1]['c.I9']+"\"}) MERGE (c1)-[:NEXT {person_id: \""+person_id+"\", start_date_d1: d1.start_date, start_date_d2: d2.start_date}]->(c2)"
		d = graphdb.run(s).data()
		
	#concepts = graphdb.run("MATCH (c1:ICD9)-[n:NEXT {person_id: \""+person_id+"\"}]->(c2:ICD9) RETURN c1.I9, n.start_date1, n.start_date2, c2.I9").data()

def process_conditions_by_person():
	people = graphdb.run("MATCH (p:Person) RETURN p.person_id").data()
	for person in people:
		set_NEXT_rels(person['p.person_id'])


def main():
	print "Reset db -- delete all nodes and edges..."
	#graphdb.run("MATCH (n) detach delete n")
	print "Inserting person..."
	load_csv_to_nodes("person.csv", "Person") 
	print "Inserting condition occurrence..."
	load_csv_to_nodes("condition_occurrence.csv", "ConditionOccurrence")
	print "Inserting ICD9 ..."
	load_csv_to_nodes("icd9.csv", "ICD9")
	
	print "Create index on Person..."
	graphdb.run("CREATE INDEX ON :Person(person_id);")
	print "Create index on ConditionOccurrence..."
	graphdb.run("CREATE INDEX ON :ConditionOccurrence(condition_occurrence_id);")
	print "Create index on ICD9..."
	graphdb.run("CREATE INDEX ON :ICD9(I9);")
	
	print "Creating relationships between person and condition occurrence..."
	load_csv_to_relationships("condition_occurrence.csv", "Person", "ConditionOccurrence", "person_id", "person_id", "condition_occurrence_id", "condition_occurrence_id", "HAS_DIAGNOSIS")
	print "Creating relationships between condition occurrence and ICD9..."
	load_csv_to_relationships("icd9.csv", "ConditionOccurrence", "ICD9", "condition_source_value", "I9", "I9", "I9", "DIAGNOSED_WITH")
	
	print "Connecting Person to ICD9 ..."
	connect_people_to_concepts()
	print "Connecting ICD9 by date of ConditionOccurrence..."
	process_conditions_by_person()

	
if __name__ == "__main__":
	main()