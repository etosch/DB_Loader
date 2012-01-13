echo "PLEASE NOTE: File names must not contains spaces. If you have difficulty connecting to the mysql server, please contact etosch@cs.umass.edu to ensure that your ip address is in the appropriate security group."
logfile_folder=$1
clojure=clojure-1.2.1.jar
if [ -e "$logfile_folder" ]
then
    project_folder=`find ~ -name DB_Loader`
    cd $project_folder
    #### Find lein ; if it isn't installed, set it up locally
    if [ "" = `which lein` ]
    then
	if ! [ -e $project_folder/lein ]
	then
	    curl https://raw.github.com/technomancy/leiningen/stable/bin/lein >> lein
	    chmod +x lein
	fi
	PATH=$PATH:$project_folder
    fi
    lein clean
    lein deps
    lein compile
    #### Set up config it it isn't already set up and read in config data
    db_config=$HOME/.db_config
    if [ -e $db_config ]
    then
	cmd="(let [m (read-string (slurp \"$db_config\"))] (list (:id m) (:data_dir m) (:user m) (:password m) (:hostname m) (:database m)))"
	config_data=(`java -cp ./lib/$clojure clojure.main -e "$cmd"`)
	id=${config_data[0]:1}
	data_dir=${config_data[1]}
	if [ $data_dir = nil ]
	then
	    data_dir="\"$HOME/data_dir/\""
	fi
	user=${config_data[2]}
	if [ $user = nil ]
	then
	    echo "~/.db_config needs to be set up. Please specify user name and then hit [ENTER]: "
	    read user
	fi
	pw=${config_data[3]}
	if [ $pw = nil ]
	then
	    echo "Please specify password and then hit [ENTER]: "
	    read pw
	fi
	host=${config_data[4]}
	if [ $host = nil ]
	then
	    echo "Please specify host and then hit [ENTER]: "
	    read host
	fi
	db=${config_data[5]:0:$((${#config_data[5]}-1))}
	if [ $db = nil ] 
	then
	    echo "Please specify database name and then hit [ENTER]: "
	    read db
	fi
    else
	data_dir=$HOME/data_dir/
	touch $db_config
	chmod 600 $db_config
	echo "~/.db_config needs to be set up. Please specify user name and then hit [ENTER]: "
	read user
	echo "Please specify password and then hit [ENTER]: "
	read pw
	echo "Please specify host and then hit [ENTER]: "
	read host
	echo "Please specify database name and then hit [ENTER]: "
	read db
	if ! [ -e $data_dir ]
	then 
	    mkdir $data_dir
	fi
	data_dir="\"$data_dir"\"
	id=nil
    fi
    query="select problem_name, problem_id from experiments group by problem_name, problem_id"
    problem_map=(`mysql -u$user -p$pw -h$host -e "\$query" $db`)
    problem_data=${problem_map[@]:2}
    maxid=(`mysql -u$user -p$pw -h$host -e "select max(id) from experiments" $db`)
    if [ $id = nil -o $id -lt ${maxid[1]} ]
    then
	echo $id, ${maxid[1]}
	id=${maxid[1]}
    fi
    echo "{:data_dir $data_dir, :user $user, :password $pw, :hostname $host, :database $db}" > $db_config
    java -cp ./lib/$clojure clojure.main -e "(spit \"$db_config\" (assoc (read-string (slurp \"$db_config\")) :id $id))"
    java -cp ./lib/$clojure clojure.main -e "(spit \"$db_config\" (assoc (read-string (slurp \"$db_config\")) :problem_data (apply hash-map (map str '($problem_data)))))"
    #### Process the files
    for f in "$logfile_folder"/*
    do
 	exists_in_db=(`mysql -u$user -p$pw -h$host -e "select count(*) from experiments where logfile_location='$f'" $db`)
 	exists_in_csv=`grep "${f:0:$((${#f}-3))}" ${data_dir:1:$((${#data_dir}-2))}experiments.csv`
 	if [ ${exists_in_db[1]} = 0 -a "$exists_in_csv" = "" ]
 	then 
 	    extension=${f:$((${#f}-3))}
 	    if [ $extension = .gz -o $extension = tgz -o $extension = log ]
 	    then
		if [ $extension = .gz ]
		then
		    gunzip $f
		    f=${f:0:$((${#f}-3))}
		fi
   		echo "Processing $f..."
 		lein run -m db_loader :filename $f
		gen_size=(`du -hm ${data_dir:1:$((${#data_dir}-2))}generations.csv`)
		if [ ${gen_size[0]} -gt 500 ]
		then
		    echo "Uploading contents of $data_dir to $db..."
		    mysqlimport --local --compress --ignore-lines=1 --replace --user=$user --password=$pw --host=$host --fields-terminated-by=',' $db ~/data_dir/experiments.csv
		    mysqlimport --local --compress --ignore-lines=1 --replace --user=$user --password=$pw --host=$host --fields-terminated-by=',' $db ~/data_dir/experiment.csv
		    mysqlimport --local --compress --ignore-lines=1 --replace --user=$user --password=$pw --host=$host --fields-terminated-by=',' $db ~/data_dir/generations.csv
 		    lein run -m db_loader :clean experiments experiment generations
		    echo "Cleaning $data_dir.."
		fi
		if [ $extension = .gz ]
		then
		    gzip $f
		fi
	    fi
	fi
    done
else
    echo "Please supply a directory containing log files to be processed. Log files extensions should be .gz, .tar, or .log"
fi