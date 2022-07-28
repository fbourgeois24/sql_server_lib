import pyodbc # Install with 'pip install pyodbc' 

""" Utilitaires pour gérer une db mariadb """
class sql_server_database():
	def __init__(self, db_name, db_server, db_port, db_user="admin", db_password=""):
		self.db = None
		self.cursor = None
		self.database = db_name
		self.host = db_server
		self.port = db_port
		self.user = db_user
		self.password = db_password


	def connect(self):
		""" Connexion à la DB """
		self.db = pyodbc.connect("DRIVER={SQL Server};SERVER=" + self.host + "," + self.port + ";DATABASE=" + self.database + ";UID=" + self.user + ";PWD=" + self.password )
		if self.db is None:
			return False
		else:
			return True

	def disconnect(self):
		""" Méthode pour déconnecter la db """
		self.db.close()

	def open(self):
		""" Méthode pour créer un curseur """
		# On essaye de fermer le curseur avant d'en recréer un 
		self.connect()
		try:
			self.cursor.close()
		except:
			pass
		self.cursor = self.db.cursor()
		if self.cursor is not None:
			return True
		else:
			return False

	def close(self, commit = False):
		""" Méthode pour détruire le curseur, avec ou sans commit """
		if commit:
			self.db.commit()
		self.cursor.close()
		self.disconnect()

	def commit(self):
		""" Méthode qui met à jour la db """
		self.db.commit()
		
	def exec(self, query, params = None, fetch = "all", autoconnect = True):
		""" Méthode pour exécuter une requête et qui ouvre et ferme  la db automatiquement """
		# Détermination du renvoi d'info ou non
		if not "SELECT" in query[:20]:
			commit = True
		else:
			commit = False
		if autoconnect:
			self.connect()
		if self.open():
			if params is not None:
				self.cursor.execute(query, params)
			else:
				self.cursor.execute(query)
			# Si pas de commit ce sera une récupération
			if not commit or "RETURNING" in query:	
				if fetch == "all":
					value = self.fetchall()
				elif fetch == "one":
					value = self.fetchone()
					# On vide le curseur pour éviter l'erreur de data restantes à la fermeture
					trash = self.fetchall()
				elif fetch == "single":
					# On essaie de prendre le premier mais si ça échoue c'est probablement que la requête n'a rien retourné
					value = self.fetchone()
					if value is not None:
						value = value[0]
				else:
					raise ValueError("Wrong fetch type")
				self.close()
				
				return value
			else:
				self.close(commit=commit)
				if autoconnect:
					self.disconnect()
		else:
			raise AttributeError("Erreur de création du curseur pour l'accès à la db")

	def fetchall(self):
		""" Méthode pour le fetchall """
		return self.cursor.fetchall()


	def fetchone(self):
		""" Méthode pour le fetchone """
		return self.cursor.fetchone()

# Test
if __name__ == "__main__":
	db = sql_server_database("BI_ODS_AIES", "10.12.84.242", "3437", db_user="aies_db2db", db_password="N<mD6h8+q*dQC(\[")

	result = db.exec(''' SELECT * FROM ds.MarketHeadpoint WHERE ID_D722_MarketHeadpoint = 1 ;''', fetch='one')
	print(result)
