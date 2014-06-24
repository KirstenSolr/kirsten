/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.indexer.kirsten;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

/**
 * Module to modify indexed content
 */
public class KirstenIndexingFilter implements IndexingFilter {
  private static final Logger LOG = LoggerFactory.getLogger(KirstenIndexingFilter.class);
  private static final String CONF_URL = "kirsten.db.url";
  private static final String CONF_DB = "kirsten.db.dbname";
  private static final String CONF_USERNAME = "kirsten.db.username";
  private static final String CONF_PASSWORD = "kirsten.db.password";
  private static final Pattern urlPattern = Pattern.compile("(http:\\/\\/|https:\\/\\/)([A-Za-z0-9.-]+(?!.*\\|\\w*$))");
  private static String confUrl;
  private static String confDB;
  private static String confUsername;
  private static String confPassword;
  private Configuration conf;
  private Connection connect = null;
  private Statement statement = null;
  private ResultSet resultSet = null;

	/**
	 * This will take the metatags that you have listed in your "urlmeta.tags"
	 * property, and looks for them inside the CrawlDatum object. If they exist,
	 * this will add it as an attribute inside the NutchDocument.
	 * 
	 * @see IndexingFilter#filter
	 */
	public NutchDocument filter(NutchDocument doc, Parse parse, Text url,
			CrawlDatum datum, Inlinks inlinks) throws IndexingException {
		if (conf != null)
			this.setConf(conf);

		if (doc == null)
			return doc;

    open();
    
    doc = addUrlBasedMetaData(doc, url);
    
    doc = addRuleBasedMetaData(parse, doc, url);
    
    // Add domain 
    doc = addDomainField(doc, url);
    
    close();
    
		return doc;
	}
  
  public NutchDocument addUrlBasedMetaData(NutchDocument doc, Text url) {
    try {
      // Statements allow to issue SQL queries to the database
      statement = connect.createStatement();
      // Result set get the result of the SQL query
      resultSet = statement
          .executeQuery("SELECT * FROM webpage WHERE url = '" + url + "'");
    } catch (Exception e) {
      LOG.error("Exception on executeQuery looking up webpage metadata", e);
    } finally {
      try {
        resultSet.next();
        //String boost = resultSet.getString("boost");
        return doc;
      }
      catch (SQLException e) {
        return doc;
      }
    }
  }

  public NutchDocument addRuleBasedMetaData(Parse parse, NutchDocument doc, Text url) {
    String title = parse.getData().getTitle();
    Float boostFactor = 1.0f;

          return doc;
/*    
    
    try {
      // Statements allow to issue SQL queries to the database
      statement = connect.createStatement();
      // Result set get the result of the SQL query
      // TODO: Test regular expressions
      resultSet = statement
          .executeQuery(
          "SELECT * FROM metadata_rules WHERE '" + url + "' LIKE CONCAT('%', rule, '%') AND type ='url' AND is_regex <> TRUE " +
          "UNION SELECT * FROM metadata_rules WHERE '" + url + "' REGEXP rule AND type ='url' AND is_regex = TRUE " +
          "UNION SELECT * FROM metadata_rules WHERE '" + title + "' LIKE CONCAT('%', rule, '%') AND type ='title' AND is_regex <> TRUE " +
          "UNION SELECT * FROM metadata_rules WHERE '" + title + "' REGEXP rule AND type ='title' AND is_regex = TRUE"
      );
    } catch (Exception e) {
      LOG.error("Exception on executeQuery (metadata_rules.*)", e);
    } finally {
      try {
        while (resultSet.next()) {
          //String boost = resultSet.getString("boost");
        }
        return doc;
      }
      catch (SQLException e) {
        return doc;
      }
    }*/
  }
  
  /**
   * Extract domain to separate field 
   */
  public NutchDocument addDomainField(NutchDocument doc, Text url) {
    Matcher matcher = urlPattern.matcher(url.toString());
    if (matcher.find()) {
      doc.add("domain", matcher.group(2));
    }
    else {
      LOG.info("Problem parsing domain for " + url);
    }
    return doc;
  }


	/** Boilerplate */
	public Configuration getConf() {
		return conf;
	}

	/**
	 * handles conf assignment and pulls the value assignment from the
	 * kirsten.db settings
	 */
  public void setConf(Configuration conf) {
    this.conf = conf;

    confUrl = conf.get(CONF_URL);
    confDB = conf.get(CONF_DB);
    confUsername = conf.get(CONF_USERNAME);
    confPassword = conf.get(CONF_PASSWORD);
  }

  // Open DB connection
  private void open() {
    try {
      Class.forName("com.mysql.jdbc.Driver");
      // Setup the connection with the DB
      connect = DriverManager
        .getConnection(confUrl + "/" + confDB, confUsername, confPassword);
    } catch (Exception e) {
      LOG.error("Error connecting to DB");
    }
  }


  // You need to close the resultSet
  private void close() {
    try {
      if (resultSet != null) {
        resultSet.close();
      }

      if (statement != null) {
        statement.close();
      }

      if (connect != null) {
        connect.close();
      }
    } catch (Exception e) {

    }
  }
}

