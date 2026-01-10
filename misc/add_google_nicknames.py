from contextlib import closing
import random
import re
import psycopg2
from psycopg2.extras import RealDictCursor
from config import PSQL_CREDENTIALS

NAME_POOLS = {
    "Afrikaans (South Africa)": {
        "MALE": [
            "Andries","Arno","Barend","Bennie","Carel","Christo","Dawid","Deon","Eben","Francois",
            "Gideon","Hendrik","Izak","Jaco","Kobus","Luan","Marinus","Nico","Ockert","Pieter",
            "Quinton","Riaan","Stefan","Theuns","Ulrich","Vernon","Wian","Xander","Yvan","Zander"
        ],
        "FEMALE": [
            "Anri","Bianca","Carli","Danika","Elna","Frané","Gretha","Hanneke","Ilze","Jana",
            "Karla","Liezl","Marli","Nadine","Odette","Petro","Retha","Suné","Tanya","Ursula",
            "Veronique","Wilna","Xaviera","Yvette","Zelda","Amoret","Bernadette","Chanté","Danelle","Elmari"
        ],
    },
    "Arabic": {
        "MALE": [
            "Ahmad","Ali","Amr","Bilal","Fadi","Faris","Hamza","Hassan","Hussein","Ibrahim",
            "Ismail","Jamal","Karim","Khaled","Mahmoud","Majed","Marwan","Mohammad","Nabil","Omar",
            "Rami","Saad","Samir","Tarek","Yasser","Youssef","Zaid","Zaki","Saleh","Adel"
        ],
        "FEMALE": [
            "Aisha","Amira","Dana","Dima","Farah","Hala","Hana","Iman","Jana","Yasmin",
            "Karima","Leila","Lina","Maha","Mariam","Mona","Nadia","Noura","Rania","Reem",
            "Salma","Samar","Sara","Salwa","Yara","Zeina","Zainab","Huda","Noor","Basma"
        ],
    },
    "Basque (Spain)": {
        "MALE": [
            "Aitor","Iker","Asier","Xabier","Mikel","Gorka","Eneko","Unai","Ander","Jon",
            "Imanol","Kepa","Ibai","Gaizka","Inaki","Julen","Oier","Peru","Xabi","Arkaitz",
            "Aritz","Inigo","Txomin","Benat","Markel","Patxi","Xanti","Ekai","Odei","Harkaitz"
        ],
        "FEMALE": [
            "Ane","Maite","Amaia","Nahia","Irati","Nekane","Nerea","Leire","Uxue","Izaro",
            "Itziar","Ainhoa","Garazi","June","Laia","Olaia","Oihana","Olatz","Maddi","Miren",
            "Maialen","Edurne","Arantxa","Lide","Nahikari","Ziara","Eider","Naia","Alazne","Lore"
        ],
    },
    "Bengali (India)": {
        "MALE": [
            "Arindam","Anirban","Soumya","Sayan","Subhasish","Abhishek","Saurav","Debojyoti","Dipanjan","Koushik",
            "Aniket","Arnab","Partha","Pradip","Prosenjit","Abir","Ayan","Ritam","Ranjan","Rahul",
            "Sumit","Sujoy","Sandeep","Indranil","Joydeep","Siddhartha","Tanmoy","Kunal","Niladri","Saptarshi"
        ],
        "FEMALE": [
            "Ananya","Sraboni","Moumita","Sanchita","Swagata","Sayantani","Rituparna","Debasmita","Ipsita","Indrani",
            "Jharna","Kakoli","Kamalika","Laboni","Lopamudra","Madhumita","Malabika","Manjima","Mitali","Nandini",
            "Nupur","Pallavi","Payel","Poulomi","Purnima","Riya","Roshni","Sangita","Shreya","Tuli"
        ],
    },
    "Bulgarian (Bulgaria)": {
        "MALE": [
            "Aleksandar","Borislav","Boyan","Dimitar","Georgi","Hristo","Ivan","Kalin","Kiril","Krasimir",
            "Lubomir","Marin","Milen","Nikola","Ognyan","Petar","Plamen","Radoslav","Rosen","Simeon",
            "Stanislav","Stoyan","Tihomir","Valentin","Velizar","Veselin","Vladislav","Yordan","Zdravko","Asen"
        ],
        "FEMALE": [
            "Aleksandra","Borislava","Desislava","Diana","Elitsa","Gergana","Ivanka","Kalina","Kameliya","Kristina",
            "Lidia","Margarita","Milena","Nadezhda","Nevena","Petya","Plamena","Radostina","Rositsa","Simona",
            "Snezhana","Stanimira","Stefka","Tsvetelina","Valentina","Velina","Vesela","Vladislava","Yana","Zornitsa"
        ],
    },
    "Catalan (Spain)": {
        "MALE": [
            "Adrià","Albert","Aleix","Àlex","Andreu","Arnau","Bernat","Carles","David","Eduard",
            "Enric","Ernest","Ferran","Francesc","Gerard","Guillem","Isaac","Jaume","Joan","Jordi",
            "Josep","Lluc","Marc","Martí","Miquel","Oriol","Pau","Pere","Pol","Roger"
        ],
        "FEMALE": [
            "Aina","Alba","Alícia","Berta","Clàudia","Clara","Cristina","Èlia","Elisabet","Emma",
            "Estel","Eva","Gemma","Helena","Ingrid","Jana","Júlia","Laia","Laura","Lia",
            "Lourdes","Marta","Mireia","Montse","Núria","Olga","Paula","Raquel","Sara","Sílvia"
        ],
    },
    "Chinese (Hong Kong)": {
        "MALE": [
            "Ka-Ho","Ka-Ming","Ka-Lok","Ka-Wai","Chi-Hin","Chi-Wai","Chi-Ming","Chun-Ho","Chun-Kit","Chun-Yiu",
            "Lok-Hei","Lok-Man","Wing-Hong","Wing-Chun","Hin-Chung","Tsz-Hin","Tsz-Kit","Tsz-Lok","Ho-Yin","Ho-Chun",
            "Yiu-Tung","Kin-Ming","Kin-Wai","Wai-Kit","Man-Chun","Pak-Hei","Cheuk-Yin","Long-Hin","Kwok-Hei","Fung-Kit"
        ],
        "FEMALE": [
            "Ka-Yan","Ka-Man","Ka-Wing","Hiu-Lam","Hiu-Ying","Hiu-Tung","Wing-Yan","Wing-Yee","Wing-Sze","Wing-Ki",
            "Tsz-Ching","Tsz-Yan","Tsz-Ying","Yan-Yi","Ka-Li","Lok-Yan","Lok-Yee","Man-Yee","Man-Yan","Hoi-Yee",
            "Sze-Yan","Sze-Wing","Sze-Yee","Yuen-Yee","Yuk-Lam","Hiu-Yan","Pui-Yee","Sin-Yi","Ching-Yi","Man-Ting"
        ],
    },
    "Czech (Czech Republic)": {
        "MALE": [
            "Jan","Jakub","Tomáš","Lukáš","Petr","Martin","Ondřej","Jiří","Michal","Marek",
            "David","Pavel","Vojtěch","Matěj","Daniel","Filip","Adam","Šimon","Josef","Roman",
            "Jaroslav","Radek","Aleš","Patrik","Karel","Vladimír","Milan","Libor","Zdeněk","Stanislav"
        ],
        "FEMALE": [
            "Anna","Eva","Tereza","Adéla","Karolína","Eliška","Veronika","Petra","Lucie","Jana",
            "Markéta","Kristýna","Barbora","Nikola","Klára","Pavla","Michaela","Alena","Monika","Zuzana",
            "Gabriela","Denisa","Kateřina","Šárka","Lenka","Natálie","Magdaléna","Sabina","Hana","Iveta"
        ],
    },
    "Danish (Denmark)": {
        "MALE": [
            "Anders","Mads","Rasmus","Jonas","Kristian","Frederik","Søren","Thomas","Jesper","Kasper",
            "Nikolaj","Emil","Magnus","Malthe","Oliver","Carl","Victor","Alexander","Tobias","Simon",
            "Jakob","Mathias","Lars","Henrik","Peter","Michael","Bo","Klaus","Mikkel","Rune"
        ],
        "FEMALE": [
            "Anne","Mette","Sofie","Camilla","Line","Maria","Louise","Ida","Emma","Laura",
            "Freja","Sara","Julie","Cecilie","Katrine","Signe","Trine","Karina","Rikke","Helle",
            "Pia","Tine","Gitte","Lene","Malene","Pernille","Stine","Alberte","Nanna","Mille"
        ],
    },
    "Dutch (Belgium)": {
        "MALE": [
            "Tom","Bram","Jeroen","Pieter","Jan","Koen","Wout","Niels","Bart","Jonas",
            "Stijn","Tim","Dries","Gert","Maarten","Ruben","Sven","Daan","Arne","Ward",
            "Laurens","Tuur","Wim","Toon","Sam","Michiel","Dieter","Jasper","Kobe","Lennert"
        ],
        "FEMALE": [
            "Annelies","Hanne","Lotte","Emma","Laura","Jolien","Sarah","Katrien","Inge","Tine",
            "Leen","Evi","Sofie","Lies","Maaike","Elke","Ilse","Els","Anne","Femke",
            "Lore","Tessa","Charlotte","Marieke","Kaat","Heleen","Jana","Noor","Ine","Axelle"
        ],
    },
    "Dutch (Netherlands)": {
        "MALE": [
            "Daan","Sem","Finn","Lucas","Levi","Milan","Bram","Thijs","Luuk","Jesse",
            "Lars","Ruben","Sven","Thomas","Julian","Mees","Niels","Timo","Koen","Max",
            "Gijs","Wouter","Sander","Bas","Joris","Floris","Pim","Mark","Bart","Nick"
        ],
        "FEMALE": [
            "Emma","Julia","Mila","Tess","Sophie","Sara","Noor","Lotte","Eva","Anna",
            "Isa","Maud","Roos","Evi","Fleur","Yara","Sanne","Lieke","Esmee","Lauren",
            "Michelle","Ilse","Anouk","Famke","Lisa","Bo","Nienke","Myrthe","Marit","Lois"
        ],
    },
    "English (Australia)": {
        "MALE": [
            "Jack","Oliver","William","Noah","Thomas","James","Lucas","Henry","Charlie","Liam",
            "Alexander","Harrison","Levi","Lachlan","Xavier","Ethan","Cooper","Hunter","Samuel","Isaac",
            "Archie","Oscar","Mason","Benjamin","Aiden","Jacob","Patrick","Harvey","Logan","Austin"
        ],
        "FEMALE": [
            "Charlotte","Olivia","Mia","Ava","Amelia","Isla","Grace","Chloe","Emily","Sophie",
            "Ruby","Lily","Harper","Evelyn","Matilda","Zoe","Sienna","Willow","Lucy","Aria",
            "Scarlett","Evie","Ellie","Hannah","Savannah","Audrey","Mackenzie","Georgia","Phoebe","Poppy"
        ],
    },
    "English (India)": {
        "MALE": [
            "Arjun","Rohan","Rahul","Abhishek","Rohit","Karthik","Siddharth","Varun","Naveen","Vivek",
            "Kiran","Amit","Sanjay","Prakash","Anand","Harish","Ajay","Vijay","Manish","Deepak",
            "Anil","Rajesh","Suresh","Sunil","Ashish","Nikhil","Sameer","Gautam","Mohan","Imran"
        ],
        "FEMALE": [
            "Aarti","Anjali","Priya","Pooja","Neha","Shreya","Kavya","Nisha","Ritu","Sneha",
            "Divya","Swati","Radhika","Megha","Suman","Reema","Komal","Payal","Jyoti","Sonali",
            "Simran","Tanya","Trisha","Pallavi","Kirti","Deepa","Shruti","Isha","Ananya","Ishita"
        ],
    },
    "English (UK)": {
        "MALE": [
            "Oliver","George","Harry","Jack","Noah","Leo","Oscar","Charlie","Jacob","Thomas",
            "William","James","Henry","Alfie","Joshua","Freddie","Archie","Alexander","Isaac","Edward",
            "Joseph","Samuel","Daniel","Ethan","Toby","Sebastian","Finn","Harvey","Reuben","Louis"
        ],
        "FEMALE": [
            "Olivia","Amelia","Isla","Ava","Mia","Emily","Sophia","Grace","Lily","Ella",
            "Freya","Charlotte","Sienna","Poppy","Daisy","Alice","Jessica","Sophie","Evelyn","Ruby",
            "Phoebe","Isabelle","Florence","Erin","Matilda","Holly","Lucy","Zoe","Millie","Aria"
        ],
    },
    "English (US)": {
        "MALE": [
            "Liam","Noah","Oliver","Elijah","James","William","Benjamin","Lucas","Henry","Alexander",
            "Mason","Michael","Ethan","Daniel","Jacob","Logan","Jackson","Levi","Sebastian","Mateo",
            "Jack","Owen","Theodore","Aiden","Samuel","Joseph","David","Carter","Wyatt","Luke","Anthony","Isaiah","Thomas","Charles","Ezra","Hudson","Gabriel","Andrew","Dylan","Asher"
        ],
        "FEMALE": [
            "Olivia","Emma","Charlotte","Amelia","Sophia","Isabella","Ava","Mia","Evelyn","Harper",
            "Luna","Camila","Gianna","Elizabeth","Eleanor","Ella","Abigail","Sofia","Avery","Scarlett",
            "Emily","Aria","Penelope","Chloe","Layla","Lily","Nora","Zoey","Grace","Victoria",
            "Madison","Riley","Hannah","Aurora","Addison","Natalie","Audrey","Brooklyn","Savannah","Claire","Stella"
        ],
    },
    "Filipino (Philippines)": {
        "MALE": [
            "Juan","Jose","Mark","John","Carlo","Paolo","Miguel","Angelo","Rafael","Gabriel",
            "Christian","Jericho","Anthony","Bryan","Kevin","Marvin","Joshua","Nathan","Francis","Aaron",
            "Adrian","Raymond","Dominic","Patrick","Vincent","Julius","Noel","Rodel","Simon","Erwin"
        ],
        "FEMALE": [
            "Maria","Ana","Anne","Angelica","Andrea","Camille","Carmina","Kristine","Katrina","Joanna",
            "Jessa","Joy","Grace","Faith","Hope","Charity","Nicole","Pauline","Patricia","May",
            "Sheila","Liza","Gloria","Sofia","Bianca","Daphne","Lorraine","Michelle","Trisha","Carla"
        ],
    },
    "Finnish (Finland)": {
        "MALE": [
            "Juhani","Mikko","Matti","Juha","Antti","Kari","Pekka","Marko","Timo","Jari",
            "Heikki","Lauri","Sami","Ville","Tommi","Petri","Janne","Aleksi","Eero","Oskari",
            "Onni","Veeti","Aapo","Arttu","Tapio","Risto","Teemu","Kalle","Ilkka","Paavo"
        ],
        "FEMALE": [
            "Maria","Anna","Helena","Kaisa","Sanna","Laura","Emilia","Aino","Iida","Noora",
            "Elisa","Veera","Suvi","Katri","Riikka","Outi","Tiina","Minna","Sari","Marja",
            "Tuuli","Hanna","Elina","Jenni","Inka","Niina","Eevi","Anni","Mira","Piritta"
        ],
    },
    "French (Canada)": {
        "MALE": [
            "Olivier","Gabriel","Thomas","Antoine","Philippe","Étienne","Samuel","William","Alexandre","Charles",
            "Maxime","Nicolas","Simon","Mathieu","Vincent","Jonathan","Sébastien","Kevin","Patrick","François",
            "Marc","Jean","Louis","David","Martin","Julien","Émile","Félix","Hugo","Tristan"
        ],
        "FEMALE": [
            "Marie","Ève","Émilie","Laurence","Camille","Chloé","Sarah","Juliette","Catherine","Isabelle",
            "Amélie","Noémie","Maude","Roxanne","Audrey","Carolane","Geneviève","Mélanie","Justine","Stéphanie",
            "Annabelle","Alexandra","Valérie","Karine","Marianne","Florence","Gabrielle","Léa","Clara","Jade"
        ],
    },
    "French (France)": {
        "MALE": [
            "Pierre","Jean","Louis","Paul","Jules","Hugo","Arthur","Lucas","Nathan","Théo",
            "Martin","Tom","Maxime","Antoine","Romain","Quentin","Benjamin","Alexis","Adrien","Julien",
            "Simon","Nicolas","Mathieu","Victor","Émile","Bastien","Clément","Léo","Raphaël","Gabriel"
        ],
        "FEMALE": [
            "Marie","Jeanne","Louise","Emma","Chloé","Manon","Camille","Léa","Zoé","Sarah",
            "Inès","Clara","Juliette","Lucie","Anaïs","Pauline","Élodie","Margot","Élise","Adèle",
            "Charlotte","Alice","Agathe","Victoire","Mathilde","Maëlle","Ambre","Romane","Gabrielle","Eva"
        ],
    },
    "Galician (Spain)": {
        "MALE": [
            "Xoán","Anxo","Brais","Iago","Antón","Xabier","Martín","Paulo","Diego","Uxío",
            "Roi","Lois","Xurxo","Alberte","Nuno","Suso","Xulio","Xacobo","Adrián","Óscar",
            "Marcos","Manuel","Ramón","Santi","Bruno","Teo","Dani","Alex","Hugo","Mauro"
        ],
        "FEMALE": [
            "Uxía","Iria","Noa","Sabela","Antía","Aldara","Lúa","Lara","Carmela","Alba",
            "Mariña","Nerea","Paula","Elisa","Marta","Andrea","Helena","Laura","Sofía","Olalla",
            "Sara","Candela","Xiana","Carla","Aroa","Berta","Ana","María","Eva","Teresa"
        ],
    },
    "German (Germany)": {
        "MALE": [
            "Lukas","Leon","Paul","Jonas","Elias","Noah","Felix","David","Tim","Jan",
            "Niklas","Fabian","Philipp","Maximilian","Benedikt","Florian","Markus","Simon","Daniel","Alexander",
            "Julian","Sebastian","Dominik","Tobias","Johannes","Martin","Kevin","Patrick","Sven","Robert"
        ],
        "FEMALE": [
            "Anna","Emma","Mia","Lea","Lena","Lara","Laura","Julia","Jana","Marie",
            "Sophie","Sarah","Nina","Lisa","Katharina","Franziska","Johanna","Clara","Amelie","Luisa",
            "Theresa","Carolin","Viktoria","Isabel","Miriam","Helena","Selina","Pia","Linda","Elisa"
        ],
    },
    "Greek (Greece)": {
        "MALE": [
            "Giorgos","Yiannis","Nikos","Dimitris","Kostas","Panagiotis","Christos","Vasilis","Stavros","Alexandros",
            "Antonis","Spiros","Thanasis","Petros","Manolis","Lefteris","Stratos","Theodoros","Haris","Aris",
            "Pavlos","Sotiris","Marios","Michalis","Tasos","Lakis","Thanos","Andreas","Stelios","Kleanthis"
        ],
        "FEMALE": [
            "Maria","Eleni","Katerina","Dimitra","Georgia","Ioanna","Sofia","Anastasia","Despina","Vasiliki",
            "Evangelia","Theodora","Konstantina","Athina","Nikoleta","Fotini","Paraskevi","Panagiota","Chrysa","Eirini",
            "Zoi","Rafaela","Elissavet","Areti","Olga","Ourania","Thomai","Antonia","Charikleia","Marina"
        ],
    },
    "Gujarati (India)": {
        "MALE": [
            "Rohan","Kunal","Dhruv","Parth","Yash","Harsh","Chirag","Jignesh","Manan","Nirav",
            "Pratik","Rahul","Sagar","Tushar","Hitesh","Nilesh","Alpesh","Jatin","Hardik","Ketan",
            "Amit","Chetan","Deepak","Kaushal","Maulik","Mehul","Sanjay","Vijay","Anand","Pritesh"
        ],
        "FEMALE": [
            "Asha","Nisha","Pooja","Rupal","Ritu","Heena","Bina","Falguni","Hiral","Janki",
            "Kiran","Kinjal","Kruti","Mansi","Naina","Neha","Pinal","Priti","Radhika","Richa",
            "Rima","Roshni","Sapna","Sejal","Shreya","Sonal","Trupti","Urvi","Vidhi","Zarna"
        ],
    },
    "Hebrew (Israel)": {
        "MALE": [
            "David","Yonatan","Yossi","Noam","Daniel","Itai","Tomer","Eitan","Amit","Omer",
            "Guy","Yuval","Nir","Barak","Roi","Shai","Ofir","Alon","Lior","Asaf",
            "Erez","Gil","Haim","Boaz","Ido","Nadav","Ziv","Tal","Ariel","Omri"
        ],
        "FEMALE": [
            "Yael","Noa","Tamar","Michal","Shira","Tali","Roni","Maya","Dana","Noga",
            "Hila","Lia","Rena","Vered","Hadar","Galit","Dafna","Rotem","Sarit","Ilana",
            "Orly","Inbar","Anat","Ronit","Avital","Meital","Orit","Carmel","Hadas","Naama"
        ],
    },
    "Hindi (India)": {
        "MALE": [
            "Arjun","Raj","Rahul","Rohit","Aman","Karan","Varun","Sandeep","Prateek","Deepak",
            "Ankit","Sumit","Vikram","Sanjay","Rakesh","Manoj","Sunil","Ashish","Pankaj","Saurabh",
            "Nitin","Gaurav","Harish","Anshul","Mohit","Vineet","Abhishek","Yogesh","Uday","Vivek"
        ],
        "FEMALE": [
            "Aarti","Pooja","Neha","Priya","Anjali","Ritu","Nidhi","Shweta","Sonali","Kavita",
            "Sneha","Radhika","Komal","Meena","Lakshmi","Kiran","Simran","Tanya","Swati","Riya",
            "Ishita","Pallavi","Kirti","Divya","Sakshi","Payal","Reema","Jyoti","Tanvi","Kriti"
        ],
    },
    "Hungarian (Hungary)": {
        "MALE": [
            "Bence","Máté","Levente","Dávid","Balázs","Gergő","Péter","Zoltán","Attila","Tamás",
            "Kristóf","András","Gábor","István","László","Zsolt","Ádám","Dániel","Norbert","Márk",
            "Ferenc","Csaba","Róbert","Richárd","Szabolcs","József","Sándor","Imre","Tibor","Miklós"
        ],
        "FEMALE": [
            "Anna","Eszter","Zsófia","Réka","Dóra","Petra","Viktória","Nóra","Orsolya","Katalin",
            "Tímea","Adrienn","Gabriella","Beáta","Julianna","Anikó","Ildikó","Ágnes","Boglárka","Csilla",
            "Mónika","Edit","Fanni","Kinga","Bernadett","Panna","Veronika","Emese","Diána","Flóra"
        ],
    },
    "Icelandic (Iceland)": {
        "MALE": [
            "Jón","Sigurður","Guðmundur","Björn","Árni","Kristján","Þór","Magnús","Einar","Stefán",
            "Ásgeir","Ragnar","Þorsteinn","Hallgrímur","Páll","Bjarni","Ólafur","Helgi","Snorri","Hannes",
            "Þórir","Gísli","Haukur","Kristófer","Andri","Aron","Freyr","Kristinn","Tómas","Róbert"
        ],
        "FEMALE": [
            "Anna","Sigríður","Guðrún","Katrín","Sara","Elín","Kristín","María","Bryndís","Hulda",
            "Ásta","Ásdís","Hildur","Sóley","Unnur","Laufey","Ragnheiður","Ingibjörg","Sunna","Rakel",
            "Tinna","Embla","Þóra","Brynja","Helga","Dagný","Eydís","Björk","Nanna","Lilja"
        ],
    },
    "Indonesian (Indonesia)": {
        "MALE": [
            "Adi","Budi","Cahyo","Dedi","Eko","Fajar","Galih","Hadi","Imam","Joko",
            "Kurnia","Lukman","Mahmud","Nugroho","Oka","Prasetyo","Rafi","Rama","Satria","Teguh",
            "Umar","Wahyu","Yanto","Zaki","Ridwan","Agus","Dimas","Rizky","Arief","Bagus"
        ],
        "FEMALE": [
            "Ayu","Bunga","Citra","Dewi","Eka","Fitri","Gita","Hana","Indah","Intan",
            "Kartika","Lestari","Mega","Nadia","Putri","Ratna","Sari","Tia","Uli","Vina",
            "Wulan","Yulia","Zahra","Nisa","Rani","Maya","Sinta","Nurul","Rizka","Amelia"
        ],
    },
    "Italian (Italy)": {
        "MALE": [
            "Luca","Marco","Alessandro","Matteo","Giuseppe","Francesco","Giorgio","Andrea","Paolo","Davide",
            "Simone","Riccardo","Federico","Stefano","Nicola","Antonio","Roberto","Pietro","Gabriele","Tommaso",
            "Salvatore","Cesare","Daniele","Enrico","Michele","Claudio","Edoardo","Carlo","Raffaele","Vincenzo"
        ],
        "FEMALE": [
            "Giulia","Sofia","Martina","Chiara","Francesca","Elena","Alessia","Sara","Federica","Valentina",
            "Ilaria","Arianna","Beatrice","Camilla","Gaia","Anna","Giorgia","Silvia","Maria","Noemi",
            "Marta","Nicole","Aurora","Bianca","Alice","Elisa","Paola","Veronica","Elisabetta","Serena"
        ],
    },
    "Japanese (Japan)": {
        "MALE": [
            "Haruto","Sota","Yuto","Yuki","Haruki","Daiki","Kaito","Riku","Kenta","Takumi",
            "Hiroto","Ryota","Yusuke","Shota","Tsubasa","Kazuya","Takahiro","Naoki","Sho","Keita",
            "Yuma","Ren","Itsuki","Kazuki","Ryusei","Yutoh","Masato","Yuji","Toru","Shinji"
        ],
        "FEMALE": [
            "Yui","Aoi","Sakura","Hina","Mio","Rin","Nanami","Miyu","Haruka","Ayaka",
            "Mei","Riko","Yuna","Kana","Nozomi","Emi","Natsuki","Sayaka","Asuka","Hinata",
            "Chihiro","Akari","Misaki","Ayumi","Erika","Keiko","Naoko","Reina","Rina","Tomomi"
        ],
    },
    "Kannada (India)": {
        "MALE": [
            "Raghav","Prakash","Vijay","Harish","Ramesh","Suresh","Shankar","Manjunath","Vikram","Arun",
            "Santosh","Karthik","Naveen","Darshan","Sudhir","Rohit","Ganesh","Raghavendra","Shivakumar","Anil",
            "Pavan","Ashok","Venkatesh","Yogesh","Uday","Deepak","Kiran","Mahesh","Ravi","Vinay"
        ],
        "FEMALE": [
            "Aishwarya","Anitha","Bhavana","Divya","Harini","Jyothi","Keerthi","Lakshmi","Meghana","Namratha",
            "Pooja","Priya","Ramya","Sangeetha","Shruthi","Sindhu","Soumya","Sunitha","Vaishnavi","Anusha",
            "Ashwini","Chaitra","Deepika","Kavya","Manjula","Neha","Pallavi","Rachana","Shreya","Vidya"
        ],
    },
    "Korean (South Korea)": {
        "MALE": [
            "Minjun","Jisoo","Jimin","Seojun","Hyunwoo","Jihun","Taeyang","Donghyun","Sungmin","Jaeho",
            "Junseo","Seungmin","Youngho","Jongwoo","Kyungsoo","Hyeonjun","Woojin","Jihoon","Byungchul","Taemin",
            "Yongjun","Sunwoo","Seungho","Jinwoo","Minseok","Sangmin","Gyumin","Jaemin","Hyeonsu","Taehyun"
        ],
        "FEMALE": [
            "Jiyeon","Soojin","Jiwoo","Minji","Yuna","Seoyeon","Haewon","Nayeon","Eunji","Hyeri",
            "Yejin","Chaeyeon","Hana","Jiwon","Sumin","Hyejin","Yuri","Jisoo","Hyejin","Seulgi",
            "Ara","Bora","Dami","Euna","Gaeun","Harin","Jina","Mina","Nari","Yunae"
        ],
    },
    "Latvian (Latvia)": {
        "MALE": [
            "Jānis","Artūrs","Edgars","Mārtiņš","Roberts","Rihards","Raitis","Aivars","Andris","Arnis",
            "Dainis","Dāvis","Edmunds","Ervīns","Gatis","Guntis","Harijs","Ilgvars","Intars","Kaspars",
            "Kristaps","Lauris","Mārcis","Nauris","Oskars","Raimonds","Sandis","Toms","Valdis","Viesturs"
        ],
        "FEMALE": [
            "Anna","Alise","Elīna","Evita","Ilze","Inese","Ieva","Jolanta","Karīna","Kristīne",
            "Lauma","Līga","Madara","Marika","Marta","Maija","Nora","Rūta","Sabīne","Sanita",
            "Sintija","Solvita","Tatjana","Undīne","Viktorija","Zane","Zinta","Agnese","Baiba","Dace"
        ],
    },
    "Lithuanian (Lithuania)": {
        "MALE": [
            "Mantas","Lukas","Domantas","Tomas","Paulius","Mindaugas","Karolis","Andrius","Dainius","Deividas",
            "Giedrius","Julius","Justinas","Kęstutis","Martynas","Rokas","Saulius","Simas","Vytautas","Algirdas",
            "Arnas","Edgaras","Eimantas","Gintaras","Ignas","Jonas","Kazimieras","Laurynas","Nerijus","Žygimantas"
        ],
        "FEMALE": [
            "Austėja","Eglė","Gabija","Ieva","Justė","Karolina","Liepa","Monika","Rūta","Simona",
            "Ugne","Viktorija","Agnė","Aistė","Aurelija","Dovilė","Greta","Ingrida","Jolita","Kristina",
            "Lina","Neringa","Ramunė","Sandra","Skirmantė","Viltė","Agnija","Aušrinė","Emilija","Goda"
        ],
    },
    "Malayalam (India)": {
        "MALE": [
            "Akhil","Anand","Arjun","Deepak","Dileep","Girish","Hari","Jithin","Kiran","Manu",
            "Nikhil","Pradeep","Rahul","Rakesh","Ranjith","Sandeep","Sanoj","Sarath","Shyam","Sreejith",
            "Sujith","Suresh","Vijay","Vineeth","Vipin","Vishnu","Ajith","Binoy","Jaison","Lijo"
        ],
        "FEMALE": [
            "Aparna","Anjali","Aswathi","Athira","Deepa","Divya","Lakshmi","Manju","Meera","Nimisha",
            "Nitya","Niya","Priya","Reshma","Revathi","Riya","Saranya","Sreelakshmi","Surya","Sneha",
            "Swathi","Veena","Vidya","Vineetha","Anu","Gayathri","Keerthi","Neethu","Remya","Soumya"
        ],
    },
    "Malay (Malaysia)": {
        "MALE": [
            "Ahmad","Adam","Aiman","Akmal","Amir","Azlan","Badrul","Danish","Faisal","Farhan",
            "Hakim","Hafiz","Hanif","Haziq","Imran","Irfan","Khairul","Lokman","Luqman","Megat",
            "Nazmi","Rafiq","Ridzuan","Shafiq","Syafiq","Syazwan","Taufik","Yusof","Zain","Zikri"
        ],
        "FEMALE": [
            "Aisyah","Aina","Alya","Amira","Balqis","Farah","Hafizah","Hana","Hani","Insyirah",
            "Izzah","Jannah","Khairunnisa","Liyana","Maisarah","Nadia","Nadira","Najwa","Nisa","Nurul",
            "Qistina","Sabrina","Siti","Sofea","Syafika","Wardah","Zahra","Zara","Zulaikha","Zurin"
        ],
    },
    "Mandarin Chinese": {
        "MALE": [
            "Wei","Jun","Ming","Lei","Hao","Yong","Bo","Tao","Qiang","Peng",
            "Jie","Jian","Chao","Liang","Feng","Rui","Zhi","Kai","Chen","Lin",
            "Yu","Zheng","Guo","Ning","Bin","Kun","Hong","Song","Xiang","Yuan"
        ],
        "FEMALE": [
            "Mei","Ying","Jing","Xia","Yan","Hua","Na","Fang","Qin","Lan",
            "Xue","Yue","Ruiyu","Qiao","Ningxin","Fen","Juan","Lian","Ping","Ting",
            "Yun","Hui","Zhen","Yimei","Wen","Shu","Ling","Qing","Suyin","Caixia"
        ],
    },
    "Marathi (India)": {
        "MALE": [
            "Akash","Amol","Aniket","Ashish","Ganesh","Girish","Harshal","Jayesh","Mahesh","Mandar",
            "Milind","Nilesh","Ninad","Omkar","Parag","Prasad","Prathamesh","Rohan","Rohit","Sachin",
            "Sagar","Sameer","Santosh","Shantanu","Shubham","Siddharth","Swapnil","Tejas","Tushar","Vaibhav"
        ],
        "FEMALE": [
            "Aarti","Anjali","Aparna","Asmita","Ashwini","Bhavana","Deepa","Gauri","Harshada","Jagruti",
            "Kavita","Ketaki","Madhuri","Manasi","Mrunali","Neha","Pallavi","Prachi","Pooja","Radhika",
            "Ranjana","Rutuja","Sakshi","Sayali","Shreya","Sneha","Sonali","Supriya","Trupti","Vaishnavi"
        ],
    },
    "Norwegian (Norway)": {
        "MALE": [
            "Anders","Bjørn","Erik","Lars","Magnus","Henrik","Jens","Ole","Nils","Sindre",
            "Stian","Thomas","Tor","Trygve","Vegard","Knut","Morten","Per","Eirik","Arne",
            "Haakon","Leif","Rune","Sverre","Trond","Pål","Simen","Øystein","Even","Kasper"
        ],
        "FEMALE": [
            "Anne","Ingrid","Kari","Marit","Liv","Astrid","Helene","Martine","Silje","Synne",
            "Camilla","Hedda","Ida","Julie","Line","Mari","Ragnhild","Solveig","Thea","Tone",
            "Tora","Tine","Unni","Åse","Elise","Nora","Oda","Sigrid","Frida","Hanne"
        ],
    },
    "Polish (Poland)": {
        "MALE": [
            "Adam","Andrzej","Bartosz","Dawid","Grzegorz","Jacek","Jan","Kacper","Karol","Krzysztof",
            "Łukasz","Maciej","Marek","Mateusz","Michał","Paweł","Piotr","Przemysław","Rafał","Ryszard",
            "Sebastian","Szymon","Tomasz","Wojciech","Zbigniew","Dominik","Hubert","Igor","Patryk","Damian"
        ],
        "FEMALE": [
            "Agnieszka","Aleksandra","Alicja","Anna","Barbara","Beata","Dominika","Dorota","Ewa","Grażyna",
            "Iwona","Joanna","Julia","Kamila","Karolina","Katarzyna","Kinga","Magdalena","Małgorzata","Marta",
            "Monika","Natalia","Paulina","Renata","Sylwia","Weronika","Wioletta","Zofia","Izabela","Helena"
        ],
    },
    "Portuguese (Brazil)": {
        "MALE": [
            "André","Bruno","Carlos","Daniel","Diego","Eduardo","Felipe","Fernando","Gabriel","Gustavo",
            "Henrique","João","José","Leonardo","Lucas","Luís","Marcelo","Marcos","Mateus","Miguel",
            "Paulo","Pedro","Rafael","Renato","Ricardo","Rodrigo","Samuel","Sérgio","Thiago","Vinícius"
        ],
        "FEMALE": [
            "Ana","Beatriz","Bruna","Camila","Carolina","Cláudia","Daniela","Débora","Elisa","Fernanda",
            "Gabriela","Isabela","Juliana","Larissa","Letícia","Luana","Luciana","Mariana","Natália","Patrícia",
            "Paula","Rafaela","Renata","Sabrina","Sara","Simone","Sônia","Taís","Vanessa","Vitória"
        ],
    },
    "Portuguese (Portugal)": {
        "MALE": [
            "António","Bruno","Carlos","Diogo","Duarte","Eduardo","Fernando","Francisco","Gonçalo","Guilherme",
            "Henrique","João","José","Luís","Manuel","Marcos","Martim","Miguel","Nuno","Paulo",
            "Pedro","Ricardo","Rui","Salvador","Sérgio","Simão","Tiago","Tomás","Vasco","Vicente"
        ],
        "FEMALE": [
            "Ana","Andreia","Beatriz","Catarina","Cláudia","Constança","Daniela","Diana","Filipa","Inês",
            "Joana","Leonor","Liliana","Luísa","Margarida","Mariana","Marta","Matilde","Patrícia","Paula",
            "Rafaela","Rita","Sara","Soraia","Sofia","Susana","Teresa","Vanessa","Vera","Vitória"
        ],
    },
    "Punjabi (India)": {
        "MALE": [
            "Amardeep","Arjun","Baldev","Bhupinder","Devinder","Gurinder","Hardeep","Harjit","Harmeet","Inderjit",
            "Jaswinder","Jatinder","Kamaljit","Kulwinder","Maninder","Manjeet","Navtej","Parminder","Pradeep","Rajinder",
            "Ravinder","Sarabjit","Sukhwinder","Surjit","Tarsem","Tejinder","Varinder","Yadwinder","Harpreet","Gurpreet"
        ],
        "FEMALE": [
            "Amanpreet","Baljinder","Charanpreet","Gurleen","Harleen","Harnoor","Harsimrat","Ishpreet","Jasleen","Jasmeet",
            "Kamalpreet","Kanika","Kirandeep","Kiranpreet","Manpreet","Navneet","Navpreet","Neelam","Prabhleen","Prabhnoor",
            "Rajdeep","Ravneet","Simran","Sukhpreet","Sukhmani","Sunaina","Tanveer","Tejdeep","Upinder","Jasnoor"
        ],
    },
    "Romanian (Romania)": {
        "MALE": [
            "Andrei","Alexandru","Bogdan","Cristian","Daniel","Florin","Gheorghe","Ion","Ionuț","Marian",
            "Marius","Mihai","Nicolae","Radu","Răzvan","Sebastian","Sorin","Ștefan","Tudor","Valentin",
            "Vasile","Victor","Vlad","Lucian","George","Alin","Adrian","Călin","Cosmin","Dragoș"
        ],
        "FEMALE": [
            "Adriana","Alina","Alexandra","Ana","Andreea","Bianca","Camelia","Carmen","Cristina","Daniela",
            "Elena","Florentina","Gabriela","Ioana","Irina","Larisa","Loredana","Lucia","Mădălina","Maria",
            "Mihaela","Monica","Oana","Raluca","Roxana","Simona","Teodora","Valentina","Violeta","Viviana"
        ],
    },
    "Russian (Russia)": {
        "MALE": [
            "Alexander","Andrei","Anton","Artem","Boris","Danila","Dmitry","Egor","Fedor","Gennady",
            "Grigory","Igor","Ilya","Ivan","Kirill","Konstantin","Maksim","Mikhail","Nikita","Oleg",
            "Pavel","Roman","Sergey","Stanislav","Timofey","Vadim","Vladimir","Vyacheslav","Yaroslav","Yuri"
        ],
        "FEMALE": [
            "Alina","Anastasia","Anna","Daria","Ekaterina","Elena","Elizaveta","Galina","Inna","Irina",
            "Karina","Ksenia","Lidiya","Marina","Maria","Natalia","Nadezhda","Olga","Olesya","Polina",
            "Sofia","Svetlana","Tatiana","Valeria","Vera","Viktoria","Yana","Yulia","Zoya","Veronika"
        ],
    },
    "Serbian (Cyrillic)": {
        "MALE": [
            "Aleksandar","Bogdan","Bojan","Branislav","Darko","Dejan","Dragan","Dušan","Goran","Igor",
            "Jovan","Lazar","Marko","Milan","Miloš","Nenad","Nikola","Predrag","Rade","Saša",
            "Siniša","Slobodan","Stefan","Uroš","Vasilije","Veljko","Vuk","Zoran","Željko","Petar"
        ],
        "FEMALE": [
            "Ana","Biljana","Bojana","Danijela","Dragana","Dušica","Gordana","Ivana","Jelena","Jovana",
            "Katarina","Ljiljana","Marija","Marina","Milica","Mirjana","Nevena","Olivera","Sanja","Slavica",
            "Snežana","Sofija","Sonja","Suzana","Tamara","Tatjana","Teodora","Vesna","Zorica","Radmila"
        ],
    },
        "Slovak (Slovakia)": {
        "MALE": [
            "Peter","Martin","Ján","Tomáš","Lukáš","Marek","Michal","Juraj","Pavol","Andrej",
            "Roman","Štefan","Marián","Branislav","Róbert","Samuel","Adam","Patrik","Filip","Matúš",
            "Richard","Viktor","Rastislav","Denis","Erik","Ľubomír","Dušan","Ľuboš","Karol","Radovan"
        ],
        "FEMALE": [
            "Lucia","Martina","Katarína","Zuzana","Monika","Petra","Jana","Veronika","Kristína","Mária",
            "Eva","Daniela","Barbora","Michaela","Alexandra","Adriana","Simona","Lenka","Natália","Nikola",
            "Ivana","Dominika","Silvia","Tatiana","Alžbeta","Andrea","Nela","Laura","Timea","Rebeka"
        ],
    },
    "Spanish (Spain)": {
        "MALE": [
            "Alejandro","Álvaro","Antonio","Carlos","Daniel","David","Diego","Enrique","Fernando","Francisco",
            "Gabriel","Guillermo","Javier","Jorge","José","Juan","Luis","Manuel","Marcos","Mario",
            "Miguel","Pablo","Pedro","Rafael","Ricardo","Roberto","Rubén","Sergio","Víctor","Adrián"
        ],
        "FEMALE": [
            "Alba","Alicia","Ana","Andrea","Ángela","Beatriz","Carmen","Carolina","Cristina","Elena",
            "Eva","Gabriela","Inés","Isabel","Laura","Lucía","María","Marta","Mercedes","Natalia",
            "Noelia","Nuria","Patricia","Paula","Raquel","Rosa","Sandra","Sara","Silvia","Sofía"
        ],
    },
    "Spanish (US)": {
        "MALE": [
            "Alejandro","Alonso","Andres","Angel","Antonio","Carlos","Cristian","Daniel","Diego","Eduardo",
            "Emilio","Erik","Fernando","Francisco","Gabriel","Hector","Ivan","Javier","Jesus","Jorge",
            "Jose","Juan","Leonardo","Luis","Marco","Miguel","Oscar","Ricardo","Roberto","Santiago"
        ],
        "FEMALE": [
            "Adriana","Alejandra","Alicia","Ana","Andrea","Angela","Brenda","Camila","Carla","Carolina",
            "Cecilia","Daniela","Diana","Elena","Elizabeth","Gabriela","Isabel","Jasmine","Jessica","Jimena",
            "Karen","Karina","Laura","Liliana","Lucia","Maria","Mariana","Monica","Nancy","Patricia"
        ],
    },
    "Swedish (Sweden)": {
        "MALE": [
            "Anders","Björn","Erik","Lars","Magnus","Henrik","Johan","Jonas","Karl","Mikael",
            "Niklas","Oskar","Patrik","Per","Peter","Robin","Simon","Stefan","Sven","Thomas",
            "Tobias","Fredrik","Gustav","Håkan","Mattias","Rasmus","Victor","Daniel","Emil","Filip"
        ],
        "FEMALE": [
            "Anna","Astrid","Cecilia","Elin","Ellen","Emma","Erika","Frida","Hanna","Helene",
            "Ingrid","Johanna","Karin","Kristina","Linnéa","Lisa","Louise","Malin","Maria","Matilda",
            "Maja","Moa","Nora","Olivia","Sara","Sofia","Stina","Therese","Tove","Viktoria"
        ],
    },
    "Tamil (India)": {
        "MALE": [
            "Arjun","Aravind","Karthik","Prakash","Suresh","Ramesh","Vignesh","Saravanan","Pranav","Senthil",
            "Manikandan","Vijay","Ajith","Balaji","Dinesh","Ganesh","Hariharan","Jagadeesh","Kishore","Lokesh",
            "Mahesh","Naveen","Pradeep","Raghu","Santhosh","Sathish","Shankar","Sridhar","Subramanian","Vasanth"
        ],
        "FEMALE": [
            "Anitha","Anjali","Aishwarya","Deepa","Divya","Gayathri","Harini","Keerthana","Kavya","Lakshmi",
            "Lavanya","Meera","Nandhini","Pavithra","Priya","Radhika","Rekha","Revathi","Sandhya","Sanjana",
            "Saranya","Sindhu","Sneha","Sridevi","Subashini","Sujatha","Swathi","Vaishnavi","Varsha","Vidhya"
        ],
    },
    "Telugu (India)": {
        "MALE": [
            "Aditya","Akhil","Anil","Aravind","Chaitanya","Charan","Gopi","Harsha","Kalyan","Kiran",
            "Krishna","Mahesh","Manoj","Naresh","Nikhil","Nithin","Pradeep","Praveen","Rakesh","Rajesh",
            "Ravi","Rohit","Sai","Sandeep","Santosh","Srinivas","Srikanth","Sumanth","Tarun","Venkatesh"
        ],
        "FEMALE": [
            "Anusha","Bhavani","Chandana","Deepika","Divya","Harika","Hema","Indu","Keerthi","Lakshmi",
            "Lavanya","Madhuri","Manasa","Meghana","Navya","Niharika","Nisha","Pooja","Pranitha","Priyanka",
            "Radhika","Ramya","Sangeetha","Shilpa","Sindhu","Sirisha","Sravani","Sridevi","Swathi","Vaishnavi"
        ],
    },
    "Thai (Thailand)": {
        "MALE": [
            "Anon","Anurak","Apichat","Aran","Arthit","Atid","Boonmee","Chai","Chaiwat","Chanin",
            "Chaowalit","Ekkachai","Ittipon","Kittisak","Korn","Krit","Manit","Narin","Nattapong","Niran",
            "Noppadon","Pakorn","Pawarit","Pisit","Prasert","Sarawut","Somchai","Suriya","Thanakorn","Wichai"
        ],
        "FEMALE": [
            "Anong","Apinya","Benjamas","Busaba","Chailai","Chanthira","Chintana","Darunee","Duangkamol","Jintana",
            "Kanya","Kannika","Kanyarat","Kanokwan","Lalita","Laksana","Malee","Mali","Naruemon","Nattaya",
            "Pensri","Phawinee","Ploy","Pornthip","Ratchanee","Rungnapa","Siriporn","Supaporn","Thanyarat","Wipa"
        ],
    },
    "Turkish (Turkey)": {
        "MALE": [
            "Ahmet","Ali","Ayhan","Barış","Burak","Can","Cem","Emre","Engin","Erdem",
            "Erhan","Fatih","Hakan","Hasan","İbrahim","Kaan","Mehmet","Murat","Mustafa","Oğuz",
            "Onur","Orhan","Ömer","Serkan","Sinan","Tolga","Uğur","Volkan","Yasin","Yılmaz"
        ],
        "FEMALE": [
            "Ahu","Aylin","Ayşe","Bahar","Beren","Buse","Ceyda","Ceren","Derya","Elif",
            "Emine","Esra","Fatma","Gizem","Gül","Gülbahar","Hatice","Havva","Melike","Melis",
            "Merve","Neslihan","Özge","Sedef","Selin","Sevgi","Sibel","Yasemin","Zeynep","Zeliha"
        ],
    },
    "Ukrainian (Ukraine)": {
        "MALE": [
            "Andriy","Bohdan","Borys","Danylo","Denys","Hryhoriy","Ihor","Ivan","Maksym","Mykhailo",
            "Mykola","Oleksandr","Oleh","Pavlo","Petro","Roman","Serhiy","Taras","Vasyl","Volodymyr",
            "Yaroslav","Yuriy","Anatoliy","Artem","Vitaliy","Viktor","Vadym","Stanislav","Valentyn","Oleksiy"
        ],
        "FEMALE": [
            "Alina","Alona","Anastasiia","Anna","Bohdana","Daria","Iryna","Kateryna","Khrystyna","Lesia",
            "Liliia","Lidiia","Mariia","Marta","Nataliia","Nadia","Oksana","Olena","Olha","Roksolana",
            "Svitlana","Taisiia","Tetiana","Viktoriia","Yana","Yuliia","Zoriana","Halyna","Inna","Zoya"
        ],
    },
    "Urdu (India)": {
        "MALE": [
            "Aamir","Abdul","Adil","Ahmed","Akhtar","Ali","Arif","Asad","Asif","Aziz",
            "Danish","Faheem","Farhan","Feroz","Hamid","Imran","Irfan","Jamal","Javed","Kaleem",
            "Kamran","Khalid","Majid","Nadeem","Nasir","Rahim","Rashid","Sajid","Shahid","Yusuf"
        ],
        "FEMALE": [
            "Aisha","Alia","Amber","Amna","Asma","Azra","Bushra","Farah","Fatima","Fiza",
            "Hina","Humaira","Iqra","Kiran","Lubna","Mehreen","Nadia","Naila","Neha","Nida",
            "Nosheen","Parveen","Rafia","Rani","Rubina","Saira","Sana","Shabnam","Sumaira","Yasmin"
        ],
    },
    "Vietnamese (Vietnam)": {
        "MALE": [
            "Anh","Bảo","Bình","Chi","Cường","Đắc","Đinh","Đức","Dương","Giang",
            "Hải","Hiếu","Hoàng","Huy","Khang","Khải","Khoa","Lâm","Long","Minh",
            "Nam","Nghĩa","Ngọc","Phong","Phúc","Quang","Sơn","Tài","Thành","Tuấn"
        ],
        "FEMALE": [
            "Anh","Bích","Chi","Diệp","Diễm","Dung","Giang","Hà","Hạnh","Hiền",
            "Hoa","Hoài","Hương","Khánh","Lan","Linh","Loan","Mai","Mỹ","Nga",
            "Ngọc","Nhi","Nhung","Phương","Quyên","Quỳnh","Thảo","Thu","Trang","Yến"
        ],
    },
}


NICK_MAX_LEN = 128
ALNUM_RE = re.compile(r"[A-Za-z]+")  # keep letter chunks only for rightmost token

def _rightmost_token(voice_name: str) -> str:
    parts = [p for p in (voice_name or "").split("-") if p]
    if not parts:
        return ""
    token = "".join(ALNUM_RE.findall(parts[-1]))
    return token.capitalize()
    
def _norm(s: str) -> str:
    return s.casefold() if isinstance(s, str) else s

def _norm_gender(g: str | None) -> str:
    g = (g or "").upper()
    if g.startswith("M"): return "MALE"
    if g.startswith("F"): return "FEMALE"
    return "NEUTRAL"

def _lang_slug(language: str) -> str:
    # Compact slug from language only (e.g., "English (US)" -> "english-us")
    slug = re.sub(r"[^A-Za-z]+", "-", language or "").strip("-").lower()
    return slug or "lang"

def _dedupe_gender_lists(pool: dict[str, list[str]]):
    m = list(dict.fromkeys(pool.get("MALE", [])))
    f = list(dict.fromkeys(pool.get("FEMALE", [])))
    overlap = set(n.lower() for n in m) & set(n.lower() for n in f)
    if overlap:
        f = [n for n in f if n.lower() not in overlap]
    pool["MALE"] = m
    pool["FEMALE"] = f

def update_google_voice_nicknames():
    with closing(psycopg2.connect(**PSQL_CREDENTIALS)) as conn:
        conn.autocommit = False
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # Ensure schema
                cur.execute("ALTER TABLE google_tts_voices ADD COLUMN IF NOT EXISTS nickname VARCHAR(%s)", (NICK_MAX_LEN,))
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS google_tts_voices_nickname_ux ON google_tts_voices (nickname)")

                # Load all voices
                cur.execute("""
                    SELECT id, voice_name, language, COALESCE(gender, '') AS gender
                    FROM google_tts_voices
                    ORDER BY language, voice_name, id
                """)
                rows = cur.fetchall()
                if not rows:
                    raise RuntimeError("No rows in google_tts_voices; run your scraper first.")

                for lang, pool in NAME_POOLS.items():
                    _dedupe_gender_lists(pool)

                used = set()  # global set of nicknames (case-insensitive)
                assignments: list[tuple[str, int]] = []  # (nickname, id)

                pending = []
                for r in rows:
                    rid   = r["id"]
                    vname = r["voice_name"] or ""
                    lang  = r["language"] or ""
                    gender = _norm_gender(r["gender"])

                    base = _rightmost_token(vname)
                    key = base.lower()
                    if len(base) >= 3 and base and key not in used:
                        used.add(key)
                        assignments.append((base, rid))
                    else:
                        pending.append((rid, vname, lang, gender))

                random.seed(42)
                counters: dict[tuple[str,str], int] = {}  # ((lang_slug, gender_tag)->int)

                for rid, vname, lang, gender in pending:
                    pool = NAME_POOLS.get(lang, {"MALE": [], "FEMALE": []})
                    male_pool = pool.get("MALE", [])
                    female_pool = pool.get("FEMALE", [])
                    cand = None

                    def take_from(lst: list[str]) -> str | None:
                        while lst:
                            n = lst.pop(0).strip()
                            if n and n.lower() not in used:
                                return n
                        return None

                    if gender == "MALE":
                        cand = take_from(male_pool)
                    elif gender == "FEMALE":
                        cand = take_from(female_pool)
                    else:
                        combined = [(n, "M") for n in male_pool] + [(n, "F") for n in female_pool]
                        random.shuffle(combined)
                        for n, tag in combined:
                            n = n.strip()
                            if n and n.lower() not in used:
                                cand = n
                                if tag == "M":
                                    male_pool.remove(n)
                                else:
                                    female_pool.remove(n)
                                break

                    if not cand:
                        slug = _lang_slug(lang)
                        gtag = {"MALE":"M","FEMALE":"F"}.get(gender, "N")
                        key2 = (slug, gtag)
                        counters[key2] = counters.get(key2, 0) + 1
                        cand = f"{slug}-{gtag}{counters[key2]:02d}"

                    cand = cand[:NICK_MAX_LEN]
                    if cand.lower() in used:
                        slug = _lang_slug(lang)
                        i = 1
                        while True:
                            alt = f"{slug}-{i}"
                            if alt.lower() not in used and len(alt) <= NICK_MAX_LEN:
                                cand = alt
                                break
                            i += 1

                    used.add(cand.lower())
                    assignments.append((cand, rid))

                if len({n.lower() for n, _ in assignments}) != len(assignments):
                    raise RuntimeError("Duplicate nicknames would be written; aborting.")

                for nick, rid in assignments:
                    cur.execute("UPDATE google_tts_voices SET nickname=%s WHERE id=%s", (nick, rid))

                conn.commit()
                print(f"Assigned {len(assignments)} unique nicknames.")
        except Exception:
            conn.rollback()
            raise

def ensure_name_pool_capacity(psql_credentials) -> dict:
    """
    Checks whether NAME_POOLS has enough names per (language, gender) to cover
    all voices that still need a nickname after considering derived candidates.
    Prints any shortages and returns a dict:
        {(language, gender): {"have": int, "need": int}}  # only for deficits
    """
    deficits = {}

    with closing(psycopg2.connect(**psql_credentials)) as conn:
        with conn.cursor() as cur:
         # Ensure schema
            cur.execute("ALTER TABLE google_tts_voices ADD COLUMN IF NOT EXISTS nickname VARCHAR(%s)", (NICK_MAX_LEN,))
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS google_tts_voices_nickname_ux ON google_tts_voices (nickname)")
            cur.execute("""
                SELECT id, language, gender, voice_name, nickname
                FROM google_tts_voices
                ORDER BY language, gender, voice_name
            """)
            rows = cur.fetchall()

    # Gather used nicknames (case-insensitive) to avoid reusing
    used_nicks = set()
    # Bucket -> list of row dicts for counting
    buckets = {}  # (language, gender) -> [ {id, voice_name, nickname, derived_ok(bool)} ]
    for (vid, lang, gender, vname, nick) in rows:
        key = (lang, (gender or "").upper() or "NEUTRAL")
        buckets.setdefault(key, [])
        item = {
            "id": vid,
            "voice_name": vname or "",
            "nickname": nick or "",
            "derived_ok": False,
        }
        if nick:
            used_nicks.add(_norm(nick))
        buckets[key].append(item)

    # Identify derived candidates that are unique and >=3 chars
    # Reserve them so we don't count them against the pool.
    derived_reserved = set()
    for key, items in buckets.items():
        for it in items:
            if it["nickname"]:
                continue
            cand = _rightmost_token(it["voice_name"])
            if cand and _norm(cand) not in used_nicks and _norm(cand) not in derived_reserved:
                it["derived_ok"] = True
                derived_reserved.add(_norm(cand))

    # Clean NAME_POOLS: remove cross-gender overlaps within each language, and
    # drop anything already used (existing nicknames) to avoid conflicts.
    cleaned_pools = {}
    for lang, genders in NAME_POOLS.items():
        # Start with gender lists (some langs may lack a gender bucket)
        male = list({n for n in genders.get("MALE", [])})
        female = list({n for n in genders.get("FEMALE", [])})
        neutral = list({n for n in genders.get("NEUTRAL", [])})

        # Remove cross-gender overlaps (case-insensitive)
        mset = {_norm(n): n for n in male}
        fset = {_norm(n): n for n in female}
        nset = {_norm(n): n for n in neutral}

        # Remove overlaps: priority to keep names in their original gender buckets
        overlap_mf = set(mset.keys()) & set(fset.keys())
        for k in overlap_mf:
            # drop from FEMALE by default; adjust if you prefer the opposite
            fset.pop(k, None)

        # Also ensure no overlaps with neutral; drop from NEUTRAL
        for k in set(nset.keys()) & set(mset.keys()):
            nset.pop(k, None)
        for k in set(nset.keys()) & set(fset.keys()):
            nset.pop(k, None)

        # Remove already-used nicknames globally
        for k in list(mset.keys()):
            if k in used_nicks:
                mset.pop(k, None)
        for k in list(fset.keys()):
            if k in used_nicks:
                fset.pop(k, None)
        for k in list(nset.keys()):
            if k in used_nicks:
                nset.pop(k, None)

        cleaned_pools[lang] = {
            "MALE": list(mset.values()),
            "FEMALE": list(fset.values()),
            "NEUTRAL": list(nset.values()),
        }

    # Tally needs vs pool availability per bucket
    for (lang, gender), items in buckets.items():
        # Count how many still need a pool-provided name:
        # those with no nickname and not derivable uniquely
        need = sum(1 for it in items if not it["nickname"] and not it["derived_ok"])

        # Available names for this bucket:
        pool_lang = cleaned_pools.get(lang)
        if not pool_lang:
            have = 0
        else:
            # Prefer same-gender pool; if empty and gender == NEUTRAL, just neutral.
            if gender in ("MALE", "FEMALE"):
                have = len(pool_lang.get(gender, [])) + len(pool_lang.get("NEUTRAL", []))
            else:
                have = len(pool_lang.get("NEUTRAL", [])) or 0

        if have < need:
            print(f"{lang} / {gender}: have {have} of {need} required")
            deficits[(lang, gender)] = {"have": have, "need": need}

    return deficits

if __name__ == "__main__":
    update_google_voice_nicknames()