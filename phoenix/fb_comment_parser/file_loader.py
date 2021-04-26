from parser_settings import ParserSettings
import os


def get_files(directory):
	for file in os.listdir(directory):
		print(file) #TODO remove this or comment it out
		yield _get_single_file(directory, file)


def _get_single_file(path:str, document:str) -> str:
	with open(os.path.join(str(path),str(document)), 'rb') as f:
		html_raw = f.read()
	return html_raw

# def get_file_list(directory):
# 	return os.listdir(directory)


#
# if __name__ == '__main__':
# 	test_directory = os.path.join(basedir, TESTDIRECTORY)
#
# 	for file in get_files(test_directory):
# 		print(file[0:20])
