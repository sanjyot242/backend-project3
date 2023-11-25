import boto3
from schemas import Class, Department, Enrollment, Dropped
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb', endpoint_url='http://localhost:5500')

# Lists of dummy names
surnames_list = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Miller', 'Davis', 'Garcia', 'Rodriguez', 'Wilson',
                 'Martinez', 'Anderson', 'Taylor', 'Thomas', 'Hernandez', 'Moore', 'Martin', 'Jackson', 'Thompson',
                 'White', 'Lopez', 'Lee', 'Gonzalez', 'Harris', 'Clark', 'Lewis', 'Robinson', 'Walker', 'Perez', 'Hall',
                 'Young', 'Allen', 'Sanchez', 'Wright', 'King', 'Scott', 'Green', 'Baker', 'Adams', 'Nelson', 'Hill',
                 'Ramirez', 'Campbell', 'Mitchell', 'Roberts', 'Carter', 'Phillips', 'Evans', 'Turner', 'Torres',
                 'Parker', 'Collins', 'Edwards', 'Stewart', 'Flores', 'Morris', 'Nguyen', 'Murphy', 'Rivera', 'Cook',
                 'Rogers', 'Morgan', 'Peterson', 'Cooper', 'Reed', 'Bailey', 'Bell', 'Gomez', 'Kelly', 'Howard', 'Ward',
                 'Cox', 'Diaz', 'Richardson', 'Wood', 'Watson', 'Brooks', 'Bennett', 'Gray', 'James', 'Reyes', 'Cruz',
                 'Hughes', 'Price', 'Myers', 'Long', 'Foster', 'Sanders', 'Ross', 'Morales', 'Powell', 'Sullivan',
                 'Russell', 'Ortiz', 'Jenkins', 'Gutierrez', 'Perry', 'Butler', 'Barnes', 'Fisher', 'Henderson',
                 'Coleman', 'Simmons', 'Patterson', 'Jordan', 'Reynolds', 'Hamilton', 'Graham', 'Kim', 'Gonzales',
                 'Alexander', 'Ramos', 'Wallace', 'Griffin', 'West', 'Cole', 'Hayes', 'Chavez', 'Gibson', 'Bryant',
                 'Ellis', 'Stevens', 'Murray', 'Ford', 'Marshall', 'Owens', 'Mcdonald', 'Harrison', 'Ruiz', 'Kennedy',
                 'Wells', 'Alvarez', 'Woods', 'Mendoza', 'Castillo', 'Olson', 'Webb', 'Washington', 'Tucker', 'Freeman',
                 'Burns', 'Henry', 'Vasquez', 'Snyder', 'Simpson', 'Crawford', 'Jimenez', 'Porter', 'Mason', 'Shaw',
                 'Gordon', 'Wagner', 'Hunter', 'Romero', 'Hicks', 'Dixon', 'Hunt', 'Palmer', 'Robertson', 'Black',
                 'Holmes', 'Stone', 'Meyer', 'Boyd', 'Mills', 'Warren', 'Fox', 'Rose', 'Rice', 'Moreno', 'Schmidt',
                 'Patel', 'Ferguson', 'Nichols', 'Herrera', 'Medina', 'Ryan', 'Fernandez', 'Weaver', 'Daniels',
                 'Stephens', 'Gardner', 'Payne', 'Kelley', 'Dunn', 'Pierce', 'Arnold', 'Tran', 'Spencer', 'Peters',
                 'Hawkins', 'Grant', 'Hansen', 'Castro', 'Hoffman', 'Hart', 'Elliott', 'Cunningham', 'Knight',
                 'Bradley', 'Carroll', 'Hudson', 'Duncan', 'Armstrong', 'Berry', 'Andrews', 'Johnston', 'Ray', 'Lane',
                 'Riley', 'Carpenter', 'Perkins', 'Aguilar', 'Silva', 'Richards', 'Willis', 'Matthews', 'Chapman',
                 'Lawrence', 'Garza', 'Vargas', 'Watkins', 'Wheeler', 'Larson', 'Carlson', 'Harper', 'George', 'Greene',
                 'Burke', 'Guzman', 'Morrison', 'Munoz', 'Jacobs', 'Obrien', 'Lawson', 'Franklin', 'Lynch', 'Bishop',
                 'Carr', 'Salazar', 'Austin', 'Mendez', 'Gilbert', 'Jensen', 'Williamson', 'Montgomery', 'Harvey',
                 'Oliver', 'Howell', 'Dean', 'Hanson', 'Weber', 'Garrett', 'Sims', 'Burton', 'Fuller', 'Soto', 'Mccoy',
                 'Welch', 'Chen', 'Schultz', 'Walters', 'Reid', 'Fields', 'Walsh', 'Little', 'Fowler', 'Bowman',
                 'Davidson', 'May', 'Day', 'Schneider', 'Newman', 'Brewer', 'Lucas', 'Holland', 'Wong', 'Banks',
                 'Santos', 'Curtis', 'Pearson', 'Delgado', 'Valdez', 'Pena', 'Rios', 'Douglas', 'Sandoval', 'Barrett',
                 'Hopkins', 'Keller', 'Guerrero', 'Stanley', 'Bates', 'Alvarado', 'Beck', 'Ortega', 'Wade', 'Estrada',
                 'Contreras', 'Barnett', 'Caldwell', 'Santiago', 'Lambert', 'Powers', 'Chambers', 'Nunez', 'Craig',
                 'Leonard', 'Lowe', 'Rhodes', 'Byrd', 'Gregory', 'Shelton', 'Frazier', 'Becker', 'Maldonado', 'Fleming',
                 'Vega', 'Sutton', 'Cohen', 'Jennings', 'Parks', 'Mcdaniel', 'Watts', 'Barker', 'Norris', 'Vaughn',
                 'Vazquez', 'Holt', 'Schwartz', 'Steele', 'Benson', 'Neal', 'Dominguez', 'Horton', 'Terry', 'Wolfe',
                 'Hale', 'Lyons', 'Graves', 'Haynes', 'Miles', 'Park', 'Warner', 'Padilla', 'Bush', 'Thornton',
                 'Mccarthy', 'Mann', 'Zimmerman', 'Erickson', 'Fletcher', 'Mckinney', 'Page', 'Dawson', 'Joseph',
                 'Marquez', 'Reeves', 'Klein', 'Espinoza', 'Baldwin', 'Moran', 'Love', 'Robbins', 'Higgins', 'Ball',
                 'Cortez', 'Le', 'Griffith', 'Bowen', 'Sharp', 'Cummings', 'Ramsey', 'Hardy', 'Swanson', 'Barber',
                 'Acosta', 'Luna', 'Chandler', 'Blair', 'Daniel', 'Cross', 'Simon', 'Dennis', 'Oconnor', 'Quinn',
                 'Gross', 'Navarro', 'Moss', 'Fitzgerald', 'Doyle', 'Mclaughlin', 'Rojas', 'Rodgers', 'Stevenson',
                 'Singh', 'Yang', 'Figueroa', 'Harmon', 'Newton', 'Paul', 'Manning', 'Garner', 'Mcgee', 'Reese',
                 'Francis', 'Burgess', 'Adkins', 'Goodman', 'Curry', 'Brady', 'Christensen', 'Potter', 'Walton',
                 'Goodwin', 'Mullins', 'Molina', 'Webster', 'Fischer', 'Campos', 'Avila', 'Sherman', 'Todd', 'Chang',
                 'Blake', 'Malone', 'Wolf', 'Hodges', 'Juarez', 'Gill', 'Farmer', 'Hines', 'Gallagher', 'Duran',
                 'Hubbard', 'Cannon', 'Miranda', 'Wang', 'Saunders', 'Tate', 'Mack', 'Hammond', 'Carrillo', 'Townsend',
                 'Wise', 'Ingram', 'Barton', 'Mejia', 'Ayala', 'Schroeder', 'Hampton', 'Rowe', 'Parsons', 'Frank',
                 'Waters', 'Strickland', 'Osborne', 'Maxwell', 'Chan', 'Deleon', 'Norman', 'Harrington', 'Casey',
                 'Patton', 'Logan', 'Bowers', 'Mueller', 'Glover', 'Floyd', 'Hartman', 'Buchanan', 'Cobb', 'French',
                 'Kramer', 'Mccormick', 'Clarke', 'Tyler', 'Gibbs', 'Moody', 'Conner', 'Sparks', 'Mcguire', 'Leon',
                 'Bauer', 'Norton', 'Pope', 'Flynn', 'Hogan', 'Robles', 'Salinas', 'Yates', 'Lindsey', 'Lloyd', 'Marsh',
                 'Mcbride', 'Owen', 'Solis', 'Pham', 'Lang', 'Pratt', 'Lara', 'Brock', 'Ballard', 'Trujillo', 'Shaffer',
                 'Drake', 'Roman', 'Aguirre', 'Morton', 'Stokes', 'Lamb', 'Pacheco', 'Patrick', 'Cochran', 'Shepherd',
                 'Cain', 'Burnett', 'Hess', 'Li', 'Cervantes', 'Olsen', 'Briggs', 'Ochoa', 'Cabrera', 'Velasquez',
                 'Montoya', 'Roth', 'Meyers', 'Cardenas', 'Fuentes', 'Weiss', 'Wilkins', 'Hoover', 'Nicholson',
                 'Underwood', 'Short', 'Carson', 'Morrow', 'Colon', 'Holloway', 'Summers', 'Bryan', 'Petersen',
                 'Mckenzie', 'Serrano', 'Wilcox', 'Carey', 'Clayton', 'Poole', 'Calderon', 'Gallegos', 'Greer', 'Rivas',
                 'Guerra', 'Decker', 'Collier', 'Wall', 'Whitaker', 'Bass', 'Flowers', 'Davenport', 'Conley', 'Houston',
                 'Huff', 'Copeland', 'Hood', 'Monroe', 'Massey', 'Roberson', 'Combs', 'Franco', 'Larsen', 'Pittman',
                 'Randall', 'Skinner', 'Wilkinson', 'Kirby', 'Cameron', 'Bridges', 'Anthony', 'Richard', 'Kirk',
                 'Bruce', 'Singleton', 'Mathis', 'Bradford', 'Boone', 'Abbott', 'Charles', 'Allison', 'Sweeney',
                 'Atkinson', 'Horn', 'Jefferson', 'Rosales', 'York', 'Christian', 'Phelps', 'Farrell', 'Castaneda',
                 'Nash', 'Dickerson', 'Bond', 'Wyatt', 'Foley', 'Chase', 'Gates', 'Vincent', 'Mathews', 'Hodge',
                 'Garrison', 'Trevino', 'Villarreal', 'Heath', 'Dalton', 'Valencia', 'Callahan', 'Hensley', 'Atkins',
                 'Huffman', 'Roy', 'Boyer', 'Shields', 'Lin', 'Hancock', 'Grimes', 'Glenn', 'Cline', 'Delacruz',
                 'Camacho', 'Dillon', 'Parrish', 'Oneill', 'Melton', 'Booth', 'Kane', 'Berg', 'Harrell', 'Pitts',
                 'Savage', 'Wiggins', 'Brennan', 'Salas', 'Marks', 'Russo', 'Sawyer', 'Baxter', 'Golden', 'Hutchinson',
                 'Liu', 'Walter', 'Mcdowell', 'Wiley', 'Rich', 'Humphrey', 'Johns', 'Koch', 'Suarez', 'Hobbs', 'Beard',
                 'Gilmore', 'Ibarra', 'Keith', 'Macias', 'Khan', 'Andrade', 'Ware', 'Stephenson', 'Henson', 'Wilkerson',
                 'Dyer', 'Mcclure', 'Blackwell', 'Mercado', 'Tanner', 'Eaton', 'Clay', 'Barron', 'Beasley', 'Oneal',
                 'Small', 'Preston', 'Wu', 'Zamora', 'Macdonald', 'Vance', 'Snow', 'Mcclain', 'Stafford', 'Orozco',
                 'Barry', 'English', 'Shannon', 'Kline', 'Jacobson', 'Woodard', 'Huang', 'Kemp', 'Mosley', 'Prince',
                 'Merritt', 'Hurst', 'Villanueva', 'Roach', 'Nolan', 'Lam', 'Yoder', 'Mccullough', 'Lester', 'Santana',
                 'Valenzuela', 'Winters', 'Barrera', 'Orr', 'Leach', 'Berger', 'Mckee', 'Strong', 'Conway', 'Stein',
                 'Whitehead', 'Bullock', 'Escobar', 'Knox', 'Meadows', 'Solomon', 'Velez', 'Odonnell', 'Kerr', 'Stout',
                 'Blankenship', 'Browning', 'Kent', 'Lozano', 'Bartlett', 'Pruitt', 'Buck', 'Barr', 'Gaines', 'Durham',
                 'Gentry', 'Mcintyre', 'Sloan', 'Rocha', 'Melendez', 'Herman', 'Sexton', 'Moon', 'Hendricks', 'Rangel',
                 'Stark', 'Lowery', 'Hardin', 'Hull', 'Sellers', 'Ellison', 'Calhoun', 'Gillespie', 'Mora', 'Knapp',
                 'Mccall', 'Morse', 'Dorsey', 'Weeks', 'Nielsen', 'Livingston', 'Leblanc', 'Mclean', 'Bradshaw',
                 'Glass', 'Middleton', 'Buckley', 'Schaefer', 'Frost', 'Howe', 'House', 'Mcintosh', 'Ho', 'Pennington',
                 'Reilly', 'Hebert', 'Mcfarland', 'Hickman', 'Noble', 'Spears', 'Conrad', 'Arias', 'Galvan',
                 'Velazquez', 'Huynh', 'Frederick', 'Randolph', 'Cantu', 'Fitzpatrick', 'Mahoney', 'Peck', 'Villa',
                 'Michael', 'Donovan', 'Mcconnell', 'Walls', 'Boyle', 'Mayer', 'Zuniga', 'Giles', 'Pineda', 'Pace',
                 'Hurley', 'Mays', 'Mcmillan', 'Crosby', 'Ayers', 'Case', 'Bentley', 'Shepard', 'Everett', 'Pugh',
                 'David', 'Mcmahon', 'Dunlap', 'Bender', 'Hahn', 'Harding', 'Acevedo', 'Raymond', 'Blackburn', 'Duffy',
                 'Landry', 'Dougherty', 'Bautista', 'Shah', 'Potts', 'Arroyo', 'Valentine', 'Meza', 'Gould', 'Vaughan',
                 'Fry', 'Rush', 'Avery', 'Herring', 'Dodson', 'Clements', 'Sampson', 'Tapia', 'Bean', 'Lynn', 'Crane',
                 'Farley', 'Cisneros', 'Benton', 'Ashley', 'Mckay', 'Finley', 'Best', 'Blevins', 'Friedman', 'Moses',
                 'Sosa', 'Blanchard', 'Huber', 'Frye', 'Krueger', 'Bernard', 'Rosario', 'Rubio', 'Mullen', 'Benjamin',
                 'Haley', 'Chung', 'Moyer', 'Choi', 'Horne', 'Yu', 'Woodward', 'Ali', 'Nixon', 'Hayden', 'Rivers',
                 'Estes', 'Mccarty', 'Richmond', 'Stuart', 'Maynard', 'Brandt', 'Oconnell', 'Hanna', 'Sanford',
                 'Sheppard', 'Church', 'Burch', 'Levy', 'Rasmussen', 'Coffey', 'Ponce', 'Faulkner', 'Donaldson',
                 'Schmitt', 'Novak', 'Costa', 'Montes', 'Booker', 'Cordova', 'Waller', 'Arellano', 'Maddox', 'Mata',
                 'Bonilla', 'Stanton', 'Compton', 'Kaufman', 'Dudley', 'Mcpherson', 'Beltran', 'Dickson', 'Mccann',
                 'Villegas', 'Proctor', 'Hester', 'Cantrell', 'Daugherty', 'Cherry', 'Bray', 'Davila', 'Rowland',
                 'Madden', 'Levine', 'Spence', 'Good', 'Irwin', 'Werner', 'Krause', 'Petty', 'Whitney', 'Baird',
                 'Hooper', 'Pollard', 'Zavala', 'Jarvis', 'Holden', 'Haas', 'Hendrix', 'Mcgrath', 'Bird', 'Lucero',
                 'Terrell', 'Riggs', 'Joyce', 'Mercer', 'Rollins', 'Galloway', 'Duke', 'Odom', 'Andersen', 'Downs',
                 'Hatfield', 'Benitez', 'Archer', 'Huerta', 'Travis', 'Mcneil', 'Hinton', 'Zhang', 'Hays', 'Mayo',
                 'Fritz', 'Branch', 'Mooney', 'Ewing', 'Ritter', 'Esparza', 'Frey', 'Braun', 'Gay', 'Riddle', 'Haney',
                 'Kaiser', 'Holder', 'Chaney', 'Mcknight', 'Gamble', 'Vang', 'Cooley', 'Carney', 'Cowan', 'Forbes',
                 'Ferrell', 'Davies', 'Barajas', 'Shea', 'Osborn', 'Bright', 'Cuevas', 'Bolton', 'Murillo', 'Lutz',
                 'Duarte', 'Kidd', 'Key', 'Cooke']
male_names_list = ['James', 'John', 'Robert', 'Michael', 'William', 'David', 'Richard', 'Charles', 'Joseph', 'Thomas',
                   'Christopher', 'Daniel', 'Paul', 'Mark', 'Donald', 'George', 'Kenneth', 'Steven', 'Edward', 'Brian',
                   'Ronald', 'Anthony', 'Kevin', 'Jason', 'Matthew', 'Gary', 'Timothy', 'Jose', 'Larry', 'Jeffrey',
                   'Frank', 'Scott', 'Eric', 'Stephen', 'Andrew', 'Raymond', 'Gregory', 'Joshua', 'Jerry', 'Dennis',
                   'Walter', 'Patrick', 'Peter', 'Harold', 'Douglas', 'Henry', 'Carl', 'Arthur', 'Ryan', 'Roger', 'Joe',
                   'Juan', 'Jack', 'Albert', 'Jonathan', 'Justin', 'Terry', 'Gerald', 'Keith', 'Samuel', 'Willie',
                   'Ralph', 'Lawrence', 'Nicholas', 'Roy', 'Benjamin', 'Bruce', 'Brandon', 'Adam', 'Harry', 'Fred',
                   'Wayne', 'Billy', 'Steve', 'Louis', 'Jeremy', 'Aaron', 'Randy', 'Howard', 'Eugene', 'Carlos',
                   'Russell', 'Bobby', 'Victor', 'Martin', 'Ernest', 'Phillip', 'Todd', 'Jesse', 'Craig', 'Alan',
                   'Shawn', 'Clarence', 'Sean', 'Philip', 'Chris', 'Johnny', 'Earl', 'Jimmy', 'Antonio', 'Danny',
                   'Bryan', 'Tony', 'Luis', 'Mike', 'Stanley', 'Leonard', 'Nathan', 'Dale', 'Manuel', 'Rodney',
                   'Curtis', 'Norman', 'Allen', 'Marvin', 'Vincent', 'Glenn', 'Jeffery', 'Travis', 'Jeff', 'Chad',
                   'Jacob', 'Lee', 'Melvin', 'Alfred', 'Kyle', 'Francis', 'Bradley', 'Jesus', 'Herbert', 'Frederick',
                   'Ray', 'Joel', 'Edwin', 'Don', 'Eddie', 'Ricky', 'Troy', 'Randall', 'Barry', 'Alexander', 'Bernard',
                   'Mario', 'Leroy', 'Francisco', 'Marcus', 'Micheal', 'Theodore', 'Clifford', 'Miguel', 'Oscar', 'Jay',
                   'Jim', 'Tom', 'Calvin', 'Alex', 'Jon', 'Ronnie', 'Bill', 'Lloyd', 'Tommy', 'Leon', 'Derek', 'Warren',
                   'Darrell', 'Jerome', 'Floyd', 'Leo', 'Alvin', 'Tim', 'Wesley', 'Gordon', 'Dean', 'Greg', 'Jorge',
                   'Dustin', 'Pedro', 'Derrick', 'Dan', 'Lewis', 'Zachary', 'Corey', 'Herman', 'Maurice', 'Vernon',
                   'Roberto', 'Clyde', 'Glen', 'Hector', 'Shane', 'Ricardo', 'Sam', 'Rick', 'Lester', 'Brent', 'Ramon',
                   'Charlie', 'Tyler', 'Gilbert', 'Gene', 'Marc', 'Reginald', 'Ruben', 'Brett', 'Angel', 'Nathaniel',
                   'Rafael', 'Leslie', 'Edgar', 'Milton', 'Raul', 'Ben', 'Chester', 'Cecil', 'Duane', 'Franklin',
                   'Andre', 'Elmer', 'Brad', 'Gabriel', 'Ron', 'Mitchell', 'Roland', 'Arnold', 'Harvey', 'Jared',
                   'Adrian', 'Karl', 'Cory', 'Claude', 'Erik', 'Darryl', 'Jamie', 'Neil', 'Jessie', 'Christian',
                   'Javier', 'Fernando', 'Clinton', 'Ted', 'Mathew', 'Tyrone', 'Darren', 'Lonnie', 'Lance', 'Cody',
                   'Julio', 'Kelly', 'Kurt', 'Allan', 'Nelson', 'Guy', 'Clayton', 'Hugh', 'Max', 'Dwayne', 'Dwight',
                   'Armando', 'Felix', 'Jimmie', 'Everett', 'Jordan', 'Ian', 'Wallace', 'Ken', 'Bob', 'Jaime', 'Casey',
                   'Alfredo', 'Alberto', 'Dave', 'Ivan', 'Johnnie', 'Sidney', 'Byron', 'Julian', 'Isaac', 'Morris',
                   'Clifton', 'Willard', 'Daryl', 'Ross', 'Virgil', 'Andy', 'Marshall', 'Salvador', 'Perry', 'Kirk',
                   'Sergio', 'Marion', 'Tracy', 'Seth', 'Kent', 'Terrance', 'Rene', 'Eduardo', 'Terrence', 'Enrique',
                   'Freddie', 'Wade']
female_names_list = ['Mary', 'Patricia', 'Linda', 'Barbara', 'Elizabeth', 'Jennifer', 'Maria', 'Susan', 'Margaret',
                     'Dorothy', 'Lisa', 'Nancy', 'Karen', 'Betty', 'Helen', 'Sandra', 'Donna', 'Carol', 'Ruth',
                     'Sharon', 'Michelle', 'Laura', 'Sarah', 'Kimberly', 'Deborah', 'Jessica', 'Shirley', 'Cynthia',
                     'Angela', 'Melissa', 'Brenda', 'Amy', 'Anna', 'Rebecca', 'Virginia', 'Kathleen', 'Pamela',
                     'Martha', 'Debra', 'Amanda', 'Stephanie', 'Carolyn', 'Christine', 'Marie', 'Janet', 'Catherine',
                     'Frances', 'Ann', 'Joyce', 'Diane', 'Alice', 'Julie', 'Heather', 'Teresa', 'Doris', 'Gloria',
                     'Evelyn', 'Jean', 'Cheryl', 'Mildred', 'Katherine', 'Joan', 'Ashley', 'Judith', 'Rose', 'Janice',
                     'Kelly', 'Nicole', 'Judy', 'Christina', 'Kathy', 'Theresa', 'Beverly', 'Denise', 'Tammy', 'Irene',
                     'Jane', 'Lori', 'Rachel', 'Marilyn', 'Andrea', 'Kathryn', 'Louise', 'Sara', 'Anne', 'Jacqueline',
                     'Wanda', 'Bonnie', 'Julia', 'Ruby', 'Lois', 'Tina', 'Phyllis', 'Norma', 'Paula', 'Diana', 'Annie',
                     'Lillian', 'Emily', 'Robin', 'Peggy', 'Crystal', 'Gladys', 'Rita', 'Dawn', 'Connie', 'Florence',
                     'Tracy', 'Edna', 'Tiffany', 'Carmen', 'Rosa', 'Cindy', 'Grace', 'Wendy', 'Victoria', 'Edith',
                     'Kim', 'Sherry', 'Sylvia', 'Josephine', 'Thelma', 'Shannon', 'Sheila', 'Ethel', 'Ellen', 'Elaine',
                     'Marjorie', 'Carrie', 'Charlotte', 'Monica', 'Esther', 'Pauline', 'Emma', 'Juanita', 'Anita',
                     'Rhonda', 'Hazel', 'Amber', 'Eva', 'Debbie', 'April', 'Leslie', 'Clara', 'Lucille', 'Jamie',
                     'Joanne', 'Eleanor', 'Valerie', 'Danielle', 'Megan', 'Alicia', 'Suzanne', 'Michele', 'Gail',
                     'Bertha', 'Darlene', 'Veronica', 'Jill', 'Erin', 'Geraldine', 'Lauren', 'Cathy', 'Joann',
                     'Lorraine', 'Lynn', 'Sally', 'Regina', 'Erica', 'Beatrice', 'Dolores', 'Bernice', 'Audrey',
                     'Yvonne', 'Annette', 'June', 'Samantha', 'Marion', 'Dana', 'Stacy', 'Ana', 'Renee', 'Ida',
                     'Vivian', 'Roberta', 'Holly', 'Brittany', 'Melanie', 'Loretta', 'Yolanda', 'Jeanette', 'Laurie',
                     'Katie', 'Kristen', 'Vanessa', 'Alma', 'Sue', 'Elsie', 'Beth', 'Jeanne', 'Vicki', 'Carla', 'Tara',
                     'Rosemary', 'Eileen', 'Terri', 'Gertrude', 'Lucy', 'Tonya', 'Ella', 'Stacey', 'Wilma', 'Gina',
                     'Kristin', 'Jessie', 'Natalie', 'Agnes', 'Vera', 'Willie', 'Charlene', 'Bessie', 'Delores',
                     'Melinda', 'Pearl', 'Arlene', 'Maureen', 'Colleen', 'Allison', 'Tamara', 'Joy', 'Georgia',
                     'Constance', 'Lillie', 'Claudia', 'Jackie', 'Marcia', 'Tanya', 'Nellie', 'Minnie', 'Marlene',
                     'Heidi', 'Glenda', 'Lydia', 'Viola', 'Courtney', 'Marian', 'Stella', 'Caroline', 'Dora', 'Jo',
                     'Vickie', 'Mattie', 'Terry', 'Maxine', 'Irma', 'Mabel', 'Marsha', 'Myrtle', 'Lena', 'Christy',
                     'Deanna', 'Patsy', 'Hilda', 'Gwendolyn', 'Jennie', 'Nora', 'Margie', 'Nina', 'Cassandra', 'Leah',
                     'Penny', 'Kay', 'Priscilla', 'Naomi', 'Carole', 'Brandy', 'Olga', 'Billie', 'Dianne', 'Tracey',
                     'Leona', 'Jenny', 'Felicia', 'Sonia', 'Miriam', 'Velma', 'Becky', 'Bobbie', 'Violet', 'Kristina',
                     'Toni', 'Misty', 'Mae', 'Shelly', 'Daisy', 'Ramona', 'Sherri', 'Erika', 'Katrina', 'Claire',
                     'Lindsey', 'Lindsay', 'Geneva', 'Guadalupe', 'Belinda', 'Margarita', 'Sheryl', 'Cora', 'Faye',
                     'Ada', 'Natasha', 'Sabrina', 'Isabel', 'Marguerite', 'Hattie', 'Harriet', 'Molly', 'Cecilia',
                     'Kristi', 'Brandi', 'Blanche', 'Sandy', 'Rosie', 'Joanna', 'Iris', 'Eunice', 'Angie', 'Inez',
                     'Lynda', 'Madeline', 'Amelia', 'Alberta', 'Genevieve', 'Monique', 'Jodi', 'Janie', 'Maggie',
                     'Kayla', 'Sonya', 'Jan', 'Lee', 'Kristine', 'Candace', 'Fannie', 'Maryann', 'Opal', 'Alison',
                     'Yvette', 'Melody', 'Luz', 'Susie', 'Olivia', 'Flora', 'Shelley', 'Kristy', 'Mamie', 'Lula',
                     'Lola', 'Verna', 'Beulah', 'Antoinette', 'Candice', 'Juana', 'Jeannette', 'Pam', 'Kelli', 'Hannah',
                     'Whitney', 'Bridget', 'Karla', 'Celia', 'Latoya', 'Patty', 'Shelia', 'Gayle', 'Della', 'Vicky',
                     'Lynne', 'Sheri', 'Marianne', 'Kara', 'Jacquelyn', 'Erma', 'Blanca', 'Myra', 'Leticia', 'Pat',
                     'Krista', 'Roxanne', 'Angelica', 'Johnnie', 'Robyn', 'Francis', 'Adrienne', 'Rosalie', 'Alexandra',
                     'Brooke', 'Bethany', 'Sadie', 'Bernadette', 'Traci', 'Jody', 'Kendra', 'Jasmine', 'Nichole',
                     'Rachael', 'Chelsea', 'Mable', 'Ernestine', 'Muriel', 'Marcella', 'Elena', 'Krystal', 'Angelina',
                     'Nadine', 'Kari', 'Estelle', 'Dianna', 'Paulette', 'Lora', 'Mona', 'Doreen', 'Rosemarie', 'Angel',
                     'Desiree', 'Antonia', 'Hope', 'Ginger', 'Janis', 'Betsy', 'Christie', 'Freda', 'Mercedes',
                     'Meredith', 'Lynette', 'Teri', 'Cristina', 'Eula', 'Leigh', 'Meghan', 'Sophia', 'Eloise',
                     'Rochelle', 'Gretchen', 'Cecelia', 'Raquel', 'Henrietta', 'Alyssa', 'Jana', 'Kelley', 'Gwen',
                     'Kerry', 'Jenna', 'Tricia', 'Laverne', 'Olive', 'Alexis', 'Tasha', 'Silvia', 'Elvira', 'Casey',
                     'Delia', 'Sophie', 'Kate', 'Patti', 'Lorena', 'Kellie', 'Sonja', 'Lila', 'Lana', 'Darla', 'May',
                     'Mindy', 'Essie', 'Mandy', 'Lorene', 'Elsa', 'Josefina', 'Jeannie', 'Miranda', 'Dixie', 'Lucia',
                     'Marta', 'Faith', 'Lela', 'Johanna', 'Shari', 'Camille', 'Tami', 'Shawna', 'Elisa', 'Ebony',
                     'Melba', 'Ora', 'Nettie', 'Tabitha', 'Ollie', 'Jaime', 'Winifred', 'Kristie', 'Marina', 'Alisha',
                     'Aimee', 'Rena', 'Myrna', 'Marla', 'Tammie', 'Latasha', 'Bonita', 'Patrice', 'Ronda', 'Sherrie',
                     'Addie', 'Francine', 'Deloris', 'Stacie', 'Adriana', 'Cheri', 'Shelby', 'Abigail', 'Celeste',
                     'Jewel', 'Cara', 'Adele', 'Rebekah', 'Lucinda', 'Dorthy', 'Chris', 'Effie', 'Trina', 'Reba',
                     'Shawn', 'Sallie', 'Aurora', 'Lenora', 'Etta', 'Lottie', 'Kerri', 'Trisha', 'Nikki', 'Estella',
                     'Francisca', 'Josie', 'Tracie', 'Marissa', 'Karin', 'Brittney', 'Janelle', 'Lourdes', 'Laurel',
                     'Helene', 'Fern', 'Elva', 'Corinne', 'Kelsey', 'Ina', 'Bettie', 'Elisabeth', 'Aida', 'Caitlin',
                     'Ingrid', 'Iva', 'Eugenia', 'Christa', 'Goldie', 'Cassie', 'Maude', 'Jenifer', 'Therese',
                     'Frankie', 'Dena', 'Lorna', 'Janette', 'Latonya', 'Candy', 'Morgan', 'Consuelo', 'Tamika',
                     'Rosetta', 'Debora', 'Cherie', 'Polly', 'Dina', 'Jewell', 'Fay', 'Jillian', 'Dorothea', 'Nell',
                     'Trudy', 'Esperanza', 'Patrica', 'Kimberley', 'Shanna', 'Helena', 'Carolina', 'Cleo', 'Stefanie',
                     'Rosario', 'Ola', 'Janine', 'Mollie', 'Lupe', 'Alisa', 'Lou', 'Maribel', 'Susanne', 'Bette',
                     'Susana', 'Elise', 'Cecile', 'Isabelle', 'Lesley', 'Jocelyn', 'Paige', 'Joni', 'Rachelle', 'Leola',
                     'Daphne', 'Alta', 'Ester', 'Petra', 'Graciela', 'Imogene', 'Jolene', 'Keisha', 'Lacey', 'Glenna',
                     'Gabriela', 'Keri', 'Ursula', 'Lizzie', 'Kirsten', 'Shana', 'Adeline', 'Mayra', 'Jayne', 'Jaclyn',
                     'Gracie', 'Sondra', 'Carmela', 'Marisa', 'Rosalind', 'Charity', 'Tonia', 'Beatriz', 'Marisol',
                     'Clarice', 'Jeanine', 'Sheena', 'Angeline', 'Frieda', 'Lily', 'Robbie', 'Shauna', 'Millie',
                     'Claudette', 'Cathleen', 'Angelia', 'Gabrielle', 'Autumn', 'Katharine', 'Summer', 'Jodie', 'Staci',
                     'Lea', 'Christi', 'Jimmie', 'Justine', 'Elma', 'Luella', 'Margret', 'Dominique', 'Socorro', 'Rene',
                     'Martina', 'Margo', 'Mavis', 'Callie', 'Bobbi', 'Maritza', 'Lucile', 'Leanne', 'Jeannine', 'Deana',
                     'Aileen', 'Lorie', 'Ladonna', 'Willa', 'Manuela', 'Gale', 'Selma', 'Dolly', 'Sybil', 'Abby',
                     'Lara', 'Dale', 'Ivy', 'Dee', 'Winnie', 'Marcy', 'Luisa', 'Jeri', 'Magdalena', 'Ofelia', 'Meagan',
                     'Audra', 'Matilda', 'Leila', 'Cornelia', 'Bianca', 'Simone', 'Bettye', 'Randi', 'Virgie',
                     'Latisha', 'Barbra', 'Georgina', 'Eliza', 'Leann', 'Bridgette', 'Rhoda', 'Haley', 'Adela', 'Nola',
                     'Bernadine', 'Flossie', 'Ila', 'Greta', 'Ruthie', 'Nelda', 'Minerva', 'Lilly', 'Terrie', 'Letha',
                     'Hilary', 'Estela', 'Valarie', 'Brianna', 'Rosalyn', 'Earline', 'Catalina', 'Ava', 'Mia',
                     'Clarissa', 'Lidia', 'Corrine', 'Alexandria', 'Concepcion', 'Tia', 'Sharron', 'Rae', 'Dona',
                     'Ericka', 'Jami', 'Elnora', 'Chandra', 'Lenore', 'Neva', 'Marylou', 'Melisa', 'Tabatha', 'Serena',
                     'Avis', 'Allie', 'Sofia', 'Jeanie', 'Odessa', 'Nannie', 'Harriett', 'Loraine', 'Penelope',
                     'Milagros', 'Emilia', 'Benita', 'Allyson', 'Ashlee', 'Tania', 'Tommie', 'Esmeralda', 'Karina',
                     'Eve', 'Pearlie', 'Zelma', 'Malinda', 'Noreen', 'Tameka', 'Saundra', 'Hillary', 'Amie', 'Althea',
                     'Rosalinda', 'Jordan', 'Lilia', 'Alana', 'Gay', 'Clare', 'Alejandra', 'Elinor', 'Michael',
                     'Lorrie', 'Jerri', 'Darcy', 'Earnestine', 'Carmella', 'Taylor', 'Noemi', 'Marcie', 'Liza',
                     'Annabelle', 'Louisa', 'Earlene', 'Mallory', 'Carlene', 'Nita', 'Selena', 'Tanisha', 'Katy',
                     'Julianne', 'John', 'Lakisha', 'Edwina', 'Maricela', 'Margery', 'Kenya', 'Dollie', 'Roxie',
                     'Roslyn', 'Kathrine', 'Nanette', 'Charmaine', 'Lavonne', 'Ilene', 'Kris', 'Tammi', 'Suzette',
                     'Corine', 'Kaye', 'Jerry', 'Merle', 'Chrystal', 'Lina', 'Deanne', 'Lilian', 'Juliana', 'Aline',
                     'Luann', 'Kasey', 'Maryanne', 'Evangeline', 'Colette', 'Melva', 'Lawanda', 'Yesenia', 'Nadia',
                     'Madge', 'Kathie', 'Eddie', 'Ophelia', 'Valeria', 'Nona', 'Mitzi', 'Mari', 'Georgette', 'Claudine',
                     'Fran', 'Alissa', 'Roseann', 'Lakeisha', 'Susanna', 'Reva', 'Deidre', 'Chasity', 'Sheree', 'Carly',
                     'James', 'Elvia', 'Alyce', 'Deirdre', 'Gena', 'Briana', 'Araceli', 'Katelyn', 'Rosanne', 'Wendi',
                     'Tessa', 'Berta', 'Marva', 'Imelda', 'Marietta', 'Marci', 'Leonor', 'Arline', 'Sasha', 'Madelyn',
                     'Janna', 'Juliette', 'Deena', 'Aurelia', 'Josefa', 'Augusta', 'Liliana', 'Young', 'Christian',
                     'Lessie', 'Amalia', 'Savannah', 'Anastasia', 'Vilma', 'Natalia', 'Rosella', 'Lynnette', 'Corina',
                     'Alfreda', 'Leanna', 'Carey', 'Amparo', 'Coleen', 'Tamra', 'Aisha', 'Wilda', 'Karyn', 'Cherry',
                     'Queen', 'Maura', 'Mai', 'Evangelina', 'Rosanna', 'Hallie', 'Erna', 'Enid', 'Mariana', 'Lacy',
                     'Juliet', 'Jacklyn', 'Freida', 'Madeleine', 'Mara', 'Hester', 'Cathryn', 'Lelia', 'Casandra',
                     'Bridgett', 'Angelita', 'Jannie', 'Dionne', 'Annmarie', 'Katina', 'Beryl', 'Phoebe', 'Millicent',
                     'Katheryn', 'Diann', 'Carissa', 'Maryellen', 'Liz', 'Lauri', 'Helga', 'Gilda', 'Adrian', 'Rhea',
                     'Marquita', 'Hollie', 'Tisha', 'Tamera', 'Angelique', 'Francesca', 'Britney', 'Kaitlin', 'Lolita',
                     'Florine', 'Rowena', 'Reyna', 'Twila', 'Fanny', 'Janell', 'Ines', 'Concetta', 'Bertie', 'Alba',
                     'Brigitte', 'Alyson', 'Vonda', 'Pansy', 'Elba', 'Noelle', 'Letitia', 'Kitty', 'Deann', 'Brandie',
                     'Louella', 'Leta', 'Felecia', 'Sharlene', 'Lesa', 'Beverley', 'Robert', 'Isabella', 'Herminia',
                     'Terra', 'Celina']

# Create a list of male first/last names
mname = []
last = 0
for male in male_names_list:
    if last >= len(surnames_list):
        last = 0
    mname.append(male + ' ' + surnames_list[last])
    last += 1

# Create a list of female first/last names
fname = []
last = len(surnames_list) - 1
for female in female_names_list:
    if last < 0:
        last = len(surnames_list) - 1
    fname.append(female + ' ' + surnames_list[last])
    last -= 1

# Combind the lists of names
last = 0
name = []
for male in mname:
    name.append(male)
    name.append(fname[last])
    last += 1

sample_classes = [
    Class(
        id=1,
        name="Web Back-End Engineering",
        course_code="449",
        section_number=1,
        current_enroll=10,
        max_enroll=30,
        department_id=2,
        instructor_id=1,
    ),
    Class(
        id=2,
        name="Web Back-End Engineering",
        course_code="449",
        section_number=2,
        current_enroll=24,
        max_enroll=30,
        department_id=2,
        instructor_id=2,
    ),
    Class(
        id=3,
        name="Web Front-End Engineering",
        course_code="349",
        section_number=1,
        current_enroll=14,
        max_enroll=30,
        department_id=2,
        instructor_id=3,
    ),
    Class(
        id=4,
        name="Introduction to Computer Science",
        course_code="120",
        section_number=1,
        current_enroll=32,
        max_enroll=30,
        department_id=2,
        instructor_id=4,
    ),
    Class(
        id=5,
        name="Calculus I",
        course_code="150A",
        section_number=1,
        current_enroll=28,
        max_enroll=30,
        department_id=4,
        instructor_id=5,
    ),
    Class(
        id=6,
        name="Calculus II",
        course_code="150B",
        section_number=1,
        current_enroll=30,
        max_enroll=30,
        department_id=3,
        instructor_id=6,
    ),
    Class(
        id=7,
        name="World History",
        course_code="181",
        section_number=1,
        current_enroll=15,
        max_enroll=30,
        department_id=6,
        instructor_id=7,
    ),
    Class(
        id=8,
        name="Anatomy & Physiology",
        course_code="211",
        section_number=1,
        current_enroll=30,
        max_enroll=30,
        department_id=7,
        instructor_id=8,
    ),
    Class(
        id=9,
        name="Earth Science",
        course_code="171",
        section_number=1,
        current_enroll=28,
        max_enroll=30,
        department_id=8,
        instructor_id=9,
    ),
    Class(
        id=10,
        name="Advanced C++",
        course_code="421",
        section_number=1,
        current_enroll=12,
        max_enroll=30,
        department_id=2,
        instructor_id=10,
    ),
    Class(
        id=11,
        name="Python Programming",
        course_code="222",
        section_number=1,
        current_enroll=27,
        max_enroll=30,
        department_id=2,
        instructor_id=11,
    ),
    Class(
        id=12,
        name="Python Programming",
        course_code="222",
        section_number=2,
        current_enroll=45,
        max_enroll=30,
        department_id=2,
        instructor_id=12,
    ),
    Class(
        id=13,
        name="Python Programming",
        course_code="222",
        section_number=3,
        current_enroll=35,
        max_enroll=30,
        department_id=2,
        instructor_id=13,
    ),
    Class(
        id=14,
        name="Python Programming",
        course_code="222",
        section_number=4,
        current_enroll=44,
        max_enroll=30,
        department_id=2,
        instructor_id=14,
    ),
]

sample_departments = [
    Department(id=1, name="CHEM"),
    Department(id=2, name="CPSC"),
    Department(id=3, name="ENGL"),
    Department(id=4, name="MATH"),
    Department(id=5, name="PHYS"),
    Department(id=6, name="HIST"),
    Department(id=7, name="BIOL"),
    Department(id=8, name="GEOL"),
]

sample_enrollments = []
place = 1
sid = 1
for class_data in sample_classes:
    while place <= class_data.current_enroll:
        sample_enrollments.append(Enrollment(
            placement=place,
            class_id=class_data.id,
            student_id=sid
        ))
        sid += 1
        place += 1
    place = 1

sample_dropped = [
    Dropped(
        class_id=2,
        student_id=1,
    ),
    Dropped(
        class_id=2,
        student_id=2,
    ),
    Dropped(
        class_id=2,
        student_id=3,
    ),
    Dropped(
        class_id=2,
        student_id=4,
    ),
]


def delete_all_dynamodb_tables():
    # List all tables
    table_list = dynamodb.tables.all()

    # Iterate over the tables and delete each one
    for table in table_list:
        print(f"Deleting table {table.name}...")
        table.delete()

        # Wait until the table is deleted
        table.wait_until_not_exists()
        print(f"Table {table.name} deleted.")


def create_class_table():
    # Define the table structure
    table = dynamodb.create_table(
        TableName='class',
        KeySchema=[
            {'AttributeName': 'id', 'KeyType': 'HASH'}  # Primary key
        ],
        AttributeDefinitions=[
            {'AttributeName': 'id', 'AttributeType': 'N'},  # Attribute type N for number
            {'AttributeName': 'department_id', 'AttributeType': 'N'},
            {'AttributeName': 'instructor_id', 'AttributeType': 'N'},
            {'AttributeName': 'available_slot', 'AttributeType': 'N'},
            {'AttributeName': 'constantGSI', 'AttributeType': 'S'},
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        },
        GlobalSecondaryIndexes=[
            {
                'IndexName': 'DepartmentIndex',
                'KeySchema': [
                    {'AttributeName': 'department_id', 'KeyType': 'HASH'}
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            },
            {
                'IndexName': 'InstructorIndex',
                'KeySchema': [

                    {'AttributeName': 'instructor_id', 'KeyType': 'HASH'}
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            },
            {
                'IndexName': 'AvailableSlotsIndex',
                'KeySchema': [
                    {'AttributeName': 'constantGSI', 'KeyType': 'HASH'},
                    {'AttributeName': 'available_slot', 'KeyType': 'RANGE'}
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            }
        ]
    )
    table.meta.client.get_waiter('table_exists').wait(TableName='class')
    print("Table created successfully.")


def create_department_table():
    table = dynamodb.create_table(
        TableName='department',
        KeySchema=[{
            'AttributeName': 'id',
            'KeyType': 'HASH'  # Partition key
        }],
        AttributeDefinitions=[{
            'AttributeName': 'id',
            'AttributeType': 'N'  # Number
        }
            # If you need to use 'id' as a GSI in another table, define the GSI here
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    table.meta.client.get_waiter('table_exists').wait(TableName='department')
    print("Table created successfully.")


def create_instructor_table():
    table = dynamodb.create_table(
        TableName='instructor',
        KeySchema=[{
            'AttributeName': 'id',
            'KeyType': 'HASH'  # Partition key
        }],
        AttributeDefinitions=[{
            'AttributeName': 'id',
            'AttributeType': 'N'  # Number
        }],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    table.meta.client.get_waiter('table_exists').wait(TableName='instructor')
    print("Table created successfully.")


def create_student_table():
    table = dynamodb.create_table(
        TableName='student',
        KeySchema=[{
            'AttributeName': 'id',
            'KeyType': 'HASH'  # Partition key
        }],
        AttributeDefinitions=[
            {
                'AttributeName': 'id',
                'AttributeType': 'N'  # Number
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    table.meta.client.get_waiter('table_exists').wait(TableName='student')
    print("Table created successfully.")


def create_dropped_table():
    table = dynamodb.create_table(
        TableName='dropped',
        KeySchema=[
            {
                'AttributeName': 'class_id',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'student_id',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'class_id',
                'AttributeType': 'N'  # Number
            },
            {
                'AttributeName': 'student_id',
                'AttributeType': 'N'  # Number
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    table.meta.client.get_waiter('table_exists').wait(TableName='dropped')
    print("Table created successfully.")


def create_enrollment_table():
    table = dynamodb.create_table(
        TableName="enrollment",
        KeySchema=[
            {
                'AttributeName': 'student_id',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'class_id',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'class_id',
                'AttributeType': 'N'  # Number
            },
            {
                'AttributeName': 'student_id',
                'AttributeType': 'N'  # Number
            },{
                'AttributeName': 'placement',
                'AttributeType': 'N'  # Number
            }
            # Additional attributes can be defined here if needed for secondary indexes
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        },
        GlobalSecondaryIndexes=[{
            'IndexName': 'ClassIndex',
            'KeySchema': [
                {
                    'AttributeName': 'class_id',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'placement',
                    'KeyType': 'RANGE'
                }
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            },
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        }]
    )
    table.meta.client.get_waiter('table_exists').wait(TableName='enrollment')
    print("Table created successfully.")


def insert_data_into_dynamodb(sample_classes):
    # Initialize the DynamoDB resource and specify the table
    table = dynamodb.Table('class')

    # Loop through your sample classes data
    for class_data in sample_classes:
        department_id = int(class_data.department_id)
        instructor_id = int(class_data.instructor_id)
        section_number = int(class_data.section_number)
        current_enroll = int(class_data.current_enroll) if class_data.current_enroll is not None else None
        max_enroll = int(class_data.max_enroll) if class_data.max_enroll is not None else None
        available_slot = max_enroll - current_enroll
        # Put item in the DynamoDB table
        response = table.put_item(
            Item={
                'id': class_data.id,
                'name': class_data.name,
                'course_code': class_data.course_code,
                'section_number': section_number,
                'current_enroll': current_enroll,
                'max_enroll': max_enroll,
                'department_id': department_id,
                'instructor_id': instructor_id,
                'available_slot': available_slot,
                'constantGSI': "ALL"
            }
        )
        print("Inserted:", response)


def insert_into_department(department_data):
    table = dynamodb.Table('department')
    for dept in department_data:
        table.put_item(
            Item={
                'id': dept.id,
                'name': dept.name
            }
        )


def insert_into_instructor(instructor_names):
    table = dynamodb.Table('instructor')
    for index, name in enumerate(instructor_names, start=1):
        table.put_item(
            Item={
                'id': index,
                'name': name
            }
        )


def insert_into_student(student_names):
    table = dynamodb.Table('student')
    for index, name in enumerate(student_names, start=1):
        table.put_item(
            Item={
                'id': index,
                'name': name,
                'waitlist_count': 0
            }
        )


def insert_into_enrollment(sample_enrollments):
    # Reference to your DynamoDB tables
    student_table = dynamodb.Table('student')
    enrollment_table = dynamodb.Table('enrollment')

    for enrollment_data in sample_enrollments:
        if enrollment_data.placement > 30:
            # Update student table
            student_table.update_item(
                Key={'id': enrollment_data.student_id},
                UpdateExpression='SET waitlist_count = :val',
                ExpressionAttributeValues={':val': 1}
            )

        # Insert into enrollment table
        enrollment_table.put_item(
            Item={
                'placement': enrollment_data.placement,
                'class_id': enrollment_data.class_id,
                'student_id': enrollment_data.student_id
            }
        )


def insert_into_dropped(sample_dropped):
    dropped_table = dynamodb.Table('dropped')

    for dropped_data in sample_dropped:
        # Insert into dropped table
        dropped_table.put_item(
            Item={
                'class_id': dropped_data.class_id,
                'student_id': dropped_data.student_id
            }
        )


def print_dynamodb_table_contents():
    # Initialize the DynamoDB resource
    # If using DynamoDB Local, include the endpoint_url parameter
    table = dynamodb.Table('class')

    # Scan the table
    response = table.scan()

    # Print each item in the response
    for item in response.get('Items', []):
        print(item)


# Call the function
delete_all_dynamodb_tables()
create_class_table()
create_department_table()
create_enrollment_table()
create_dropped_table()
create_instructor_table()
create_student_table()

insert_data_into_dynamodb(sample_classes)
insert_into_department(sample_departments)
insert_into_instructor(name[500:])
insert_into_student(name[:500])
insert_into_enrollment(sample_enrollments)
insert_into_dropped(sample_dropped)
print_dynamodb_table_contents()


def update_class_and_enrollments(class_updates, enrollments, student_update):
    try:
        class_table = dynamodb.Table('class')
        enrollment_table = dynamodb.Table('enrollment')
        student_table = dynamodb.Table('student')

        # Updating class table
        for class_update in class_updates:
            class_id, current_enroll = class_update
            class_table.update_item(
                Key={'id': class_id},
                UpdateExpression='SET current_enroll = :val',
                ExpressionAttributeValues={':val': current_enroll}
            )

        # Inserting into enrollment table
        for enrollment in enrollments:
            placement, class_id, student_id = enrollment
            enrollment_table.put_item(
                Item={
                    'placement': placement,
                    'class_id': class_id,
                    'student_id': student_id
                }
            )

        # Updating student table
        student_id, waitlist_count = student_update
        student_table.update_item(
            Key={'id': student_id},
            UpdateExpression='SET waitlist_count = :val',
            ExpressionAttributeValues={':val': waitlist_count}
        )

        print("Updates and inserts completed successfully.")

    except ClientError as e:
        print(e.response['Error']['Message'])


# Define your updates and inserts
class_updates = [(8, 31), (4, 33), (13, 36)]
enrollments = [(31, 8, 1), (33, 4, 1), (36, 13, 1)]
student_update = (1, 2)

# Execute the function
update_class_and_enrollments(class_updates, enrollments, student_update)
