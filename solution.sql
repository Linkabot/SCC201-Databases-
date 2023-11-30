CREATE TABLE A (e VARCHAR, q VARCHAR, PRIMARY KEY (e));
CREATE TABLE F (i INT , a INT , t VARCHAR ,  PRIMARY KEY (a));
CREATE TABLE K (d VARCHAR, u VARCHAR , PRIMARY KEY(d));
CREATE TABLE R (c INT, p INT, k DATE , l VARCHAR , a_I INT, a_B INT, PRIMARY KEY(p), FOREIGN KEY(a_I) REFERENCES F(a), FOREIGN KEY(a_B) REFERENCES F(a));
CREATE TABLE L (b VARCHAR, l VARCHAR, t VARCHAR , p INT, j VARCHAR, PRIMARY KEY (b), FOREIGN KEY (t) REFERENCES F(t), FOREIGN KEY (p) REFERENCES R(p));
CREATE TABLE G (h VARCHAR , p INT, a INT , f VARCHAR , PRIMARY KEY(p) FOREIGN KEY(p) REFERENCES R(p), FOREIGN KEY (a) REFERENCES F(a));
CREATE TABLE S (g INT, p INT, r VARCHAR,  PRIMARY KEY(g, r), FOREIGN KEY(p) REFERENCES R(p));
CREATE TABLE T (e VARCHAR, a INT, p INT, FOREIGN KEY(e) REFERENCES A(e), FOREIGN KEY(a) REFERENCES F(a), FOREIGN KEY(p) REFERENCES R(p));
CREATE TABLE H (a INT, d VARCHAR, FOREIGN KEY(a) REFERENCES F(a),  FOREIGN KEY(d) REFERENCES K(d));
CREATE TABLE E (d VARCHAR, p INT, FOREIGN KEY(d) REFERENCES K(d), FOREIGN KEY(p) REFERENCES R(p));
CREATE TABLE Q (d VARCHAR, b VARCHAR, FOREIGN KEY(d) REFERENCES K(d), FOREIGN KEY(b)  REFERENCES L(b));


INSERT OR IGNORE INTO G(h,p,a,f) VALUES ("Salu Digby", 16, 12, "Shrinking Violet");
INSERT OR IGNORE INTO G(h,p,a,f) VALUES ("Imra Ardeen", 3, 22, "Saturn Girl");
INSERT OR IGNORE INTO G(h,p,a,f) VALUES ("Condo Arlik", 28, 16, "Chemical King");
INSERT OR IGNORE INTO G(h,p,a,f) VALUES ("Nura Nal", 23, 14, "Dream Girl");
INSERT OR IGNORE INTO G(h,p,a,f) VALUES ("Thom Kallor", 10, 25, "Star Boy");

INSERT OR IGNORE INTO K(d,u) VALUES ("Kl", "Klingon");
INSERT OR IGNORE INTO K(d,u) VALUES ("O2", "Obfiscation");
INSERT OR IGNORE INTO K(d,u) VALUES ("Ob", "Obduron");
INSERT OR IGNORE INTO K(d,u) VALUES ("En", "English");
INSERT OR IGNORE INTO K(d,u) VALUES ("S2", "Spartican");

INSERT OR IGNORE INTO R(c,p,k,l) VALUES (22, 28, "2000-01-01", "Condo Arlik");
INSERT OR IGNORE INTO R(c,p,k,l) VALUES (22, 17, "2000-01-01", "Chuck Taine");
INSERT OR IGNORE INTO R(c,p,k,l) VALUES (22, 40, "2000-01-01", "Nobody");
INSERT OR IGNORE INTO R(c,p,k,l) VALUES (2, 16, "2000-01-01", "Salu Digby");
INSERT OR IGNORE INTO R(c,p,k,l) VALUES (22, 27, "2000-01-01", "Tasmia Mallor");

INSERT OR IGNORE INTO F(a,t) VALUES (8, "Dryad");
INSERT OR IGNORE INTO F(i,a,t) VALUES (27, 12, "Imsk");
INSERT OR IGNORE INTO F(a,t) VALUES (10, "Earth");
INSERT OR IGNORE INTO F(i,a,t) VALUES (8, 24, "Winath");
INSERT OR IGNORE INTO F(i,a,t) VALUES (27, 19, "Talok VIII");

INSERT OR IGNORE INTO S(g,p,r) VALUES (6, 3, "ability to read and control minds");
INSERT OR IGNORE INTO S(g,p,r) VALUES (51, 14, "Super-hearing");
INSERT OR IGNORE INTO S(g,p,r) VALUES (67, 19, "Flight");
INSERT OR IGNORE INTO S(g,p,r) VALUES (84, 22, "control and generation of electrical fields");
INSERT OR IGNORE INTO S(g,p,r) VALUES (107, 35, "Spellcasting");

INSERT OR IGNORE INTO L(b,t) VALUES ("Darkseid", "Apocalypse");
INSERT OR IGNORE INTO L(b,t) VALUES ("Mission on non-existant planet", "Zorgorn");
INSERT OR IGNORE INTO L(b,t) VALUES ("Planet Kidnap", "Daxam");
INSERT OR IGNORE INTO L(b,t) VALUES ("Earth War", "Earth");


INSERT OR IGNORE INTO R(c, p, l, k, a_I) VALUES (14, 89, 'Lincoln', '2001-05-16', 40);
INSERT OR IGNORE INTO R(c, p, l, k, a_I) VALUES (89, 1, 'George', '2003-06-20', 20);
INSERT OR IGNORE INTO R(c, p, l, k, a_I) VALUES (56, 63, 'Steve', '2001-05-16', 40);
INSERT OR IGNORE INTO R(c, p, l, k, a_I) VALUES (64, 265, 'Paul', '2003-06-20', 20);

INSERT INTO G(p,h,f) VALUES (101,'Eric Noel','Santa * Clause');
INSERT OR IGNORE INTO R(p,l,k) VALUES(101, 'Eric Noel', '2001-12-25');


INSERT OR IGNORE INTO F(i,a,t) VALUES (0, 40, "Pluto");
INSERT OR IGNORE INTO F(i,a,t) VALUES (0, 20, "Earth");

CREATE INDEX person_age ON R(c);
CREATE INDEX person_born ON R(a_B);
CREATE INDEX person_lives ON R(a_I);
CREATE INDEX person_name ON R(l);
CREATE INDEX hero_name ON G(f);
CREATE INDEX language_name ON K(u);
CREATE INDEX Powers_abilities ON S(r);
CREATE INDEX Planet_name ON F(t);
CREATE INDEX population_index ON F(i);
CREATE UNIQUE INDEX IF NOT EXISTS planets_id ON F ( a );

CREATE VIEW [JuniorHERO] as SELECT h  as secretIdentity from R  natural join G where R.c  <21;
CREATE VIEW [hero] as SELECT h as heroName From R natural join G;
CREATE VIEW [hero_mission] as SELECT b as mission , p as heroId From L natural join G; 
CREATE VIEW [speaks] as SELECT l as personName , u as languages From R natural join K  natural join E;
CREATE VIEW [person_job] as SELECT * From R natural join F natural join A natural join T;
