import urllib2
from igraph import *
from multiprocessing import Process, Lock
from thread import start_new_thread
import socket


#HOST_URL = "http://172.16.18.230:8080/"
HOST_URL = "http://172.16.18.217:8080/"
OBJECTS_API = "/api/sector/%d/objects"
ROOTS_API = "/api/sector/%d/roots"
COLLECT_API = "/api/sector/%d/company/GEPARDY/trajectory"

def list_unique(seq):
   keys = {}
   for e in seq:
       keys[e] = 1
   return keys.keys()

def get_roots(sector):
	url = "%s%s" % (HOST_URL, ROOTS_API % sector)

	res = urllib2.urlopen(url)

	nodes = []
	for line in res.readlines():
		nodes.append(int(line))
	
	return nodes


def get_objects_and_edges(sector, l):
	url = "%s%s" % (HOST_URL, OBJECTS_API % sector)

	l.acquire()
	try:
		res = urllib2.urlopen(url, timeout = 10)
	except socket.timeout:
		res = urllib2.urlopen(url)
	l.release()

	objects = []
	edges = []
	
	for line in res.readlines():
		edges.append( [int(s) for s in line.split(' ')] )
		objects.append(int(line.split(' ')[0]))
		objects.append(int(line.split(' ')[1]))

	objects = list_unique(objects)
	
	return [objects, edges]

def build_graph(sector, l):
	g = Graph(directed = True)
	
	objects_and_edges = get_objects_and_edges(sector, l)
	roots = get_roots(sector)	
	
	objects = objects_and_edges[0]

	vertices_count = max(objects)
	max_from_roots = max(roots)
	if vertices_count < max_from_roots:
		vertices_count = max_from_roots

	g.add_vertices(vertices_count + 1)
	g.add_edges(objects_and_edges[1])

	return [g, objects, roots]


def get_collectable(g, objects, roots):
	copy = list(objects)

	for root_id in roots:
		for target in g.subcomponent(root_id, mode=OUT):
			try:			
				copy.remove(target)
			except ValueError:
				pass

	return copy


def update_collectable(collectable, sector):
	if collectable:
		url = "%s%s" % (HOST_URL, OBJECTS_API % sector)

		objects = []

		response = urllib2.urlopen(url)

		for line in response.readlines():
			objects.append(int(line.split(' ')[0]))
			objects.append(int(line.split(' ')[1]))

		objects = list_unique(objects)

		for collectable_id in collectable:
			if collectable_id not in objects:
				collectable.remove(collectable_id)


done_sectors = [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]

def rebuild(g, collectable, sector):
	if collectable:
		url = "%s%s" % (HOST_URL, OBJECTS_API % sector)

		objects = []
		edges = []

		response = urllib2.urlopen(url)

		for line in response.readlines():
			edges.append( [int(s) for s in line.split(' ')] )
			objects.append(int(line.split(' ')[0]))
			objects.append(int(line.split(' ')[1]))

		objects = list_unique(objects)

		g.delete_edges(None)
		g.add_edges(edges)

		for collectable_id in collectable:
			if collectable_id not in objects:
				collectable.remove(collectable_id)
	

def turbo_request(trajectory, sector, l):
	try:
		done_sectors[sector - 1] = 0
		l.acquire()
		urllib2.urlopen(HOST_URL + COLLECT_API%sector, "trajectory=%s"%trajectory)
		done_sectors[sector - 1] = 1
		l.release()
	except urllib2.HTTPError:
		turbo_request(trajectory, sector, l)

def collect(g, collectable, sector, lock):
	max_len = 0
	max_path = []	
	max_point1 = 0
	max_point2 = 0

	lock = Lock()

	if not done_sectors[sector - 1]:
		rebuild(g, collectable, sector)

	for index1, point1 in enumerate(collectable):
		for index2, point2 in enumerate(collectable):
			if index1 == index2:
				continue

			if not done_sectors[sector - 1] or max_len < 2:
				paths = g.get_shortest_paths(point1, point2)
				if any(paths):
					path = paths[0]
					path_len = len(path)	

					if path_len > max_len:
						max_path = list(path)
						max_len = path_len
						max_point1 = point1
						max_point2 = point2						
			else:	
				trajectory = ' '.join(map(str, max_path))
				start_new_thread(turbo_request,(trajectory, sector, lock))
				for item in max_path:	
					try:
						collectable.remove(item)
					except ValueError:
						pass
						
				g.delete_edges(g.incident(max_point1, mode="ALL"))
				g.delete_edges(g.incident(max_point2, mode="ALL"))
				return 1


	if max_len > 2:
		trajectory = ' '.join(map(str, max_path))
		start_new_thread(turbo_request,(trajectory, sector, lock))

		if not done_sectors[sector - 1]:
			rebuild(g, collectable, sector)


		for item in max_path:	
			try:
				collectable.remove(item)
			except ValueError:
				pass

		g.delete_edges(g.incident(max_point1, mode="ALL"))
		g.delete_edges(g.incident(max_point2, mode="ALL"))
		return 1

	return 0
					

def first_time_opener(g, collectable, sector, lock):
	collectable_len = len(collectable)


	for index1, point1 in reversed(list(enumerate(collectable))):
		for index2, point2 in enumerate(collectable):
			if index1 == index2:
				continue

			paths = g.get_shortest_paths(point1, point2)
			if any(paths):
				path = paths[0]
				path_len = len(path)
	
				if path_len > 20 or path_len > collectable_len / 6:
					trajectory = ' '.join(map(str, path))
					start_new_thread(turbo_request,(trajectory, sector, lock))

					for item in path:	
						try:
							collectable.remove(item)
						except ValueError:
							pass

					g.delete_edges(g.incident(point1, mode="ALL"))
					g.delete_edges(g.incident(point2, mode="ALL"))
					return 1

	return 0
				

def collect_sector(sector, l):

	print "Process for sector %d launched." % sector
	graph_base = build_graph(sector, l)
	g = graph_base[0]
	collectable = get_collectable(g, graph_base[1], graph_base[2])

	lock = Lock()

	first_time_opener(g, collectable, sector, lock)
	while(collect(g, collectable, sector, lock)):
		pass

	
	COLLECT_URL = "%s%s" % (HOST_URL, COLLECT_API % sector)

	while collectable:
		update_collectable(collectable, sector)
		if collectable:
			try: 
				target = collectable.pop()
				urllib2.urlopen(COLLECT_URL, "trajectory=%d" % target)
			except urllib2.HTTPError:
				update_collectable(collectable, sector)
				if collectable:
					urllib2.urlopen(COLLECT_URL, "trajectory=%d"%collectable.pop())
	

if __name__ == '__main__':

	processes = []
	lock = Lock()

	for i in range(1, 11):
		processes.append(Process(target=collect_sector, args=(i, lock)))

	for p in processes:
		p.start()

	for p in processes:
		p.join()

