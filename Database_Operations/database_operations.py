from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import csv

class database_operations:
    """
        Description : This class will be used for connecting and operating the database functions and stuff. Like, connection
        to the databse, inserting, extracting data etc.

        Database : Cassandra
        Version : 1.0

    """

    def __init__(self, logger, path):
        try:
            self.log = logger
            self.path = path

            cloud_config = {
                 'secure_connect_bundle': './secure-connect-ineuron.zip'
             }
            auth_provider = PlainTextAuthProvider('qSwRACZazKenlCysedgYtiQw',
                                                   'DPnnxGOXHw3iI,M5G33mg7cMbokuAjPsPX2pXsojIMZrbK1iPJn86sin8ESzD9ILEpG3b7dtkOumvwQZlMRKeGz-cWn234skZ9zhfGThxh-4NEvZgL97snqy2CJk.gIh')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            self.session = cluster.connect()

            self.session.execute(""" CREATE TABLE if not exists ineuron.phishing_dataset(
                     qty_dot_url int PRIMARY KEY, qty_hyphen_url int, qty_underline_url int, qty_slash_url int, qty_questionmark_url int, qty_equal_url int, qty_at_url int, qty_and_url int, qty_exclamation_url int, qty_space_url int, qty_tilde_url int, qty_comma_url int, qty_plus_url int, qty_asterisk_url int, qty_hashtag_url int, qty_dollar_url int, qty_percent_url int, qty_tld_url int,  length_url int, qty_dot_domain int, qty_hyphen_domain int, qty_underline_domain int, qty_slash_domain int, qty_questionmark_domain int, qty_equal_domain int, qty_at_domain int, qty_and_domain int, qty_exclamation_domain int, qty_space_domain int, qty_tilde_domain int, qty_comma_domain int, qty_plus_domain int, qty_asterisk_domain int, qty_hashtag_domain int, qty_dollar_domain int, qty_percent_domain int, qty_vowels_domain int, domain_length int, domain_in_ip int, server_client_domain int, qty_dot_directory int, qty_hyphen_directory int, qty_underline_directory int, qty_slash_directory int, qty_questionmark_directory int, qty_equal_directory int, qty_at_directory int, qty_and_directory int, qty_exclamation_directory int, qty_space_directory int, qty_tilde_directory int, qty_comma_directory int, qty_plus_directory int, qty_asterisk_directory int, qty_hashtag_directory int, qty_dollar_directory int, qty_percent_directory int, directory_length int, qty_dot_file int, qty_hyphen_file int, qty_underline_file int, qty_slash_file int, qty_questionmark_file int, qty_equal_file int, qty_at_file int, qty_and_file int, qty_exclamation_file int, qty_space_file int, qty_tilde_file int, qty_comma_file int, qty_plus_file int, qty_asterisk_file int, qty_hashtag_file int, qty_dollar_file int, qty_percent_file int, file_length int, qty_dot_params int, qty_hyphen_params int, qty_underline_params int, qty_slash_params int, qty_questionmark_params int, qty_equal_params int, qty_at_params int, qty_and_params int, qty_exclamation_params int, qty_space_params int, qty_tilde_params int, qty_comma_params int, qty_plus_params int, qty_asterisk_params int, qty_hashtag_params int, qty_dollar_params int, qty_percent_params int, params_length int, tld_present_params int, qty_params int, email_in_url int, time_response int, domain_spf int, asn_ip int, time_domain_activation int, time_domain_expiration int, qty_ip_resolved int, qty_nameservers int, qty_mx_servers int, ttl_hostname int, tls_ssl_certificate int, qty_redirects int, url_google_index int, domain_google_index int, url_shortened int, phishing int);""")

            self.session.execute('use ineuron;')
            self.log.info('Database Updated Successfully.')

        except Exception as e:
            self.log.warning('An error has occurred : {}'.format(e))
            raise Exception

    def uploading_data(self):
        try:
            csv_reader = csv.reader(open(self.path))
            next(csv_reader)
            # Uploading the dataset in the database.
            
            for rows in csv_reader:
                query = """INSERT INTO ineuron.phishing_dataset(qty_dot_url, qty_hyphen_url, qty_underline_url, qty_slash_url, qty_questionmark_url, qty_equal_url, qty_at_url, qty_and_url, qty_exclamation_url, qty_space_url, qty_tilde_url, qty_comma_url, qty_plus_url, qty_asterisk_url, qty_hashtag_url, qty_dollar_url, qty_percent_url, qty_tld_url,  length_url, qty_dot_domain, qty_hyphen_domain, qty_underline_domain, qty_slash_domain, qty_questionmark_domain, qty_equal_domain, qty_at_domain, qty_and_domain, qty_exclamation_domain, qty_space_domain, qty_tilde_domain, qty_comma_domain, qty_plus_domain, qty_asterisk_domain, qty_hashtag_domain, qty_dollar_domain, qty_percent_domain, qty_vowels_domain, domain_length, domain_in_ip, server_client_domain, qty_dot_directory, qty_hyphen_directory, qty_underline_directory, qty_slash_directory, qty_questionmark_directory, qty_equal_directory, qty_at_directory, qty_and_directory, qty_exclamation_directory, qty_space_directory, qty_tilde_directory, qty_comma_directory, qty_plus_directory, qty_asterisk_directory, qty_hashtag_directory, qty_dollar_directory, qty_percent_directory, directory_length, qty_dot_file, qty_hyphen_file, qty_underline_file, qty_slash_file, qty_questionmark_file, qty_equal_file, qty_at_file, qty_and_file , qty_exclamation_file, qty_space_file, qty_tilde_file, qty_comma_file, qty_plus_file, qty_asterisk_file, qty_hashtag_file, qty_dollar_file, qty_percent_file, file_length, qty_dot_params, qty_hyphen_params, qty_underline_params, qty_slash_params, qty_questionmark_params, qty_equal_params, qty_at_params, qty_and_params, qty_exclamation_params, qty_space_params, qty_tilde_params, qty_comma_params, qty_plus_params, qty_asterisk_params, qty_hashtag_params, qty_dollar_params, qty_percent_params, params_length, tld_present_params, qty_params, email_in_url, time_response, domain_spf, asn_ip, time_domain_activation, time_domain_expiration, qty_ip_resolved, qty_nameservers, qty_mx_servers, ttl_hostname, tls_ssl_certificate, qty_redirects, url_google_index, domain_google_index, url_shortened, phishing)
                VALUES('%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d','%d');""" %(
                    int(rows[1]), int(rows[2]), int(rows[3]), int(rows[4]),int(rows[5]), int(rows[6]), int(rows[7]), int(rows[8]),int(rows[9]), int(rows[10]), int(rows[11]), int(rows[12]),int(rows[13]), int(rows[14]), int(rows[15]), int(rows[16]),int(rows[17]), int(rows[18]), int(rows[19]), int(rows[20]),int(rows[21]), int(rows[22]), int(rows[23]), int(rows[24]), int(rows[25]), int(rows[26]), int(rows[27]), int(rows[28]), int(rows[29]), int(rows[30]), int(rows[31]), int(rows[32]), int(rows[33]), int(rows[34]), int(rows[35]), int(rows[36]), int(rows[37]), int(rows[38]), int(rows[39]), int(rows[40]), int(rows[41]), int(rows[42]), int(rows[43]), int(rows[44]), int(rows[45]), int(rows[46]), int(rows[47]), int(rows[48]), int(rows[49]), int(rows[50]), int(rows[51]), int(rows[52]),int(rows[53]), int(rows[54]), int(rows[55]), int(rows[56]), int(rows[57]), int(rows[58]), int(rows[59]), int(rows[60]), int(rows[61]), int(rows[62]), int(rows[63]), int(rows[64]), int(rows[65]), int(rows[66]), int(rows[67]), int(rows[68]), int(rows[69]), int(rows[70]), int(rows[71]), int(rows[72]), int(rows[73]), int(rows[74]), int(rows[75]), int(rows[76]), int(rows[77]), int(rows[78]), int(rows[79]), int(rows[80]), int(rows[81]), int(rows[82]), int(rows[83]), int(rows[84]),int(rows[85]), int(rows[86]), int(rows[87]), int(rows[88]),int(rows[89]), int(rows[90]), int(rows[91]), int(rows[92]),int(rows[93]), int(rows[94]), int(rows[95]), int(rows[96]),int(rows[97]), int(rows[98]), int(rows[99]), int(rows[100]), int(rows[101]), int(rows[102]), int(rows[103]), int(rows[104]), int(rows[105]), int(rows[106]), int(rows[107]), int(rows[108]),int(rows[109]), int(rows[110]), int(rows[111]), int(rows[112]))
                self.session.execute(query)
            self.log.info('Dataset Uploaded Successfully.')

        except Exception as e:
            self.log.warning('An error has occurred : {}'.format(e))
            raise Exception

    def extracting_data(self):
        try:
            # Getting the training file ready.
            dataset_path = 'dataset_full.csv'
            training = open(dataset_path, 'w', encoding='utf8')
            training.truncate()
            write = csv.writer(training)
            
            # Writing the column names of the dataset.
            write.writerow(tuple(['qty_dot_url, qty_hyphen_url, qty_underline_url, qty_slash_url, qty_questionmark_url, qty_equal_url, qty_at_url, qty_and_url, qty_exclamation_url, qty_space_url, qty_tilde_url, qty_comma_url, qty_plus_url, qty_asterisk_url, qty_hashtag_url, qty_dollar_url, qty_percent_url, qty_tld_url,  length_url, qty_dot_domain, qty_hyphen_domain, qty_underline_domain, qty_slash_domain, qty_questionmark_domain, qty_equal_domain, qty_at_domain, qty_and_domain, qty_exclamation_domain, qty_space_domain, qty_tilde_domain, qty_comma_domain, qty_plus_domain, qty_asterisk_domain, qty_hashtag_domain, qty_dollar_domain, qty_percent_domain, qty_vowels_domain, domain_length, domain_in_ip, server_client_domain, qty_dot_directory, qty_hyphen_directory, qty_underline_directory, qty_slash_directory, qty_questionmark_directory, qty_equal_directory, qty_at_directory, qty_and_directory, qty_exclamation_directory, qty_space_directory, qty_tilde_directory, qty_comma_directory, qty_plus_directory, qty_asterisk_directory, qty_hashtag_directory, qty_dollar_directory, qty_percent_directory, directory_length, qty_dot_file, qty_hyphen_file, qty_underline_file, qty_slash_file, qty_questionmark_file, qty_equal_file, qty_at_file, qty_and_file , qty_exclamation_file, qty_space_file, qty_tilde_file, qty_comma_file, qty_plus_file, qty_asterisk_file, qty_hashtag_file, qty_dollar_file, qty_percent_file, file_length, qty_dot_params, qty_hyphen_params, qty_underline_params, qty_slash_params, qty_questionmark_params, qty_equal_params, qty_at_params, qty_and_params, qty_exclamation_params, qty_space_params, qty_tilde_params, qty_comma_params, qty_plus_params, qty_asterisk_params, qty_hashtag_params, qty_dollar_params, qty_percent_params, params_length, tld_present_params, qty_params, email_in_url, time_response, domain_spf, asn_ip, time_domain_activation, time_domain_expiration, qty_ip_resolved, qty_nameservers, qty_mx_servers, ttl_hostname, tls_ssl_certificate, qty_redirects, url_google_index, domain_google_index, url_shortened, phishing']))
            for val in self.session.execute('select * from ineuron.phishing_dataset;'):
                 row = [val.qty_dot_url, val.qty_hyphen_url, val.qty_underline_url, val.qty_slash_url, val.qty_questionmark_url, val.qty_equal_url, val.qty_at_url, val.qty_and_url, val.qty_exclamation_url, val.qty_space_url, val.qty_tilde_url, val.qty_comma_url, val.qty_plus_url, val.qty_asterisk_url, val.qty_hashtag_url, val.qty_dollar_url, val.qty_percent_url, val.qty_tld_url,  val.length_url, val.qty_dot_domain, val.qty_hyphen_domain, val.qty_underline_domain, val.qty_slash_domain, val.qty_questionmark_domain, val.qty_equal_domain, val.qty_at_domain, val.qty_and_domain, val.qty_exclamation_domain, val.qty_space_domain, val.qty_tilde_domain, val.qty_comma_domain, val.qty_plus_domain, val.qty_asterisk_domain, val.qty_hashtag_domain, val.qty_dollar_domain, val.qty_percent_domain, val.qty_vowels_domain, val.domain_length, val.domain_in_ip, val.server_client_domain, val.qty_dot_directory, val.qty_hyphen_directory, val.qty_underline_directory, val.qty_slash_directory, val.qty_questionmark_directory, val.qty_equal_directory, val.qty_at_directory, val.qty_and_directory, val.qty_exclamation_directory, val.qty_space_directory, val.qty_tilde_directory, val.qty_comma_directory, val.qty_plus_directory, val.qty_asterisk_directory, val.qty_hashtag_directory, val.qty_dollar_directory, val.qty_percent_directory, val.directory_length, val.qty_dot_file, val.qty_hyphen_file, val.qty_underline_file, val.qty_slash_file, val.qty_questionmark_file, val.qty_equal_file, val.qty_at_file, val.qty_and_file , val.qty_exclamation_file, val.qty_space_file, val.qty_tilde_file, val.qty_comma_file, val.qty_plus_file, val.qty_asterisk_file, val.qty_hashtag_file, val.qty_dollar_file, val.qty_percent_file, val.file_length, val.qty_dot_params, val.qty_hyphen_params, val.qty_underline_params, val.qty_slash_params, val.qty_questionmark_params, val.qty_equal_params, val.qty_at_params, val.qty_and_params, val.qty_exclamation_params, val.qty_space_params, val.qty_tilde_params, val.qty_comma_params, val.qty_plus_params, val.qty_asterisk_params, val.qty_hashtag_params, val.qty_dollar_params, val.qty_percent_params, val.params_length, val.tld_present_params, val.qty_params, val.email_in_url, val.time_response, val.domain_spf, val.asn_ip, val.time_domain_activation, val.time_domain_expiration, val.qty_ip_resolved, val.qty_nameservers, val.qty_mx_servers, val.ttl_hostname, val.tls_ssl_certificate, val.qty_redirects, val.url_google_index, val.domain_google_index, val.url_shortened, val.phishing]
                 # Putting all the rows in the csv file
                 write.writerow(tuple(row))
            
            training.close()
            self.log.info('Operation Successful. {}'.format(dataset_path))
            return dataset_path

        except Exception as e:
            self.log.warning('An errorr has occurred : {}'.format(e))
            raise Exception

    def start_db(self):
        self.uploading_data()
        train_path = self.extracting_data()
        return train_path