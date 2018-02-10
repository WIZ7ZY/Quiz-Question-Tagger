CREATE TABLE IF NOT EXISTS 'student' (
'idStudent' int(11) NOT NULL AUTO_INCREMENT,
'idClass' int(11) NOT NULL,
'firstname' varchar(20) COLLATE utf8_unicode_ci NOT NULL,
'lastname' varchar(20) COLLATE utf8_unicode_ci NOT NULL,
'age' int(11) NOT NULL,
PRIMARY KEY('idStudent) 
)ENGINE = MyISAM DEFAULT CHARSET = utf8_unicode_ci AUTO_INCREMENT=7 ;