import zlib

str_object1 = open(r'd:\vincere\data_output\bwd\upload_folder\cv_14468', 'rb').read()
str_object2 = zlib.decompress(str_object1)
f = open('my_recovered_log_file', 'wb')
f.write(str_object2)
f.close()
