cont = 0

line1 = ""
line2 = ""
val = True

for line in open("movies.txt", 'r'):
	cont = cont + 1

	if cont % 9 == 4:
		try:
			aux = line.replace(',', 'a').split(' ')[1]
			num1 = aux.split('/')[0]
			num2 = aux.split('/')[1]
			line1 = str(float(num1) / float(num2)) + ","
		except:
			val = False
	elif cont % 9 == 8:
		if val:
			print line1, line.replace(',', 'a').replace('\n', '')
	elif cont % 9 == 0:
		cont = 0
		val = True

	
