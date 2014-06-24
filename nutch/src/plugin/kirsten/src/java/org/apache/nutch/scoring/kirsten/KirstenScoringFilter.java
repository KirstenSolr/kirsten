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

package org.apache.nutch.scoring.kirsten;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Iterator;
import java.util.List;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.crawl.Inlinks;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.scoring.ScoringFilter;
import org.apache.nutch.scoring.ScoringFilterException;

/**
 * For documentation:
 * 
 * @see KirstenIndexingFilter
 */
public class KirstenScoringFilter implements ScoringFilter {

  private static final Logger LOG = LoggerFactory.getLogger(KirstenScoringFilter.class);
  private static final String CONF_URL = "kirsten.db.url";
  private static final String CONF_DB = "kirsten.db.dbname";
  private static final String CONF_USERNAME = "kirsten.db.username";
  private static final String CONF_PASSWORD = "kirsten.db.password";
  private static String confUrl;
  private static String confDB;
  private static String confUsername;
  private static String confPassword;
  private Configuration conf;
  private Connection connect = null;
  private Statement statement = null;
  private ResultSet resultSet = null;

  public Configuration getConf() {
    return conf;
  }

  public void setConf(Configuration conf) {
    this.conf = conf;

    confUrl = conf.get(CONF_URL);
    confDB = conf.get(CONF_DB);
    confUsername = conf.get(CONF_USERNAME);
    confPassword = conf.get(CONF_PASSWORD);
  }


  /**
   * 
   * @see ScoringFilter#updateDbScore
   */

  /** Increase the score by a sum of inlinked scores. */
  public void updateDbScore(Text url, CrawlDatum old, CrawlDatum datum, List inlinked)
    throws ScoringFilterException {
  }

  /**
   * Takes the metadata, specified in your "urlmeta.tags" property, from the
   * datum object and injects it into the content. This is transfered to the
   * parseData object.
   * 
   * @see ScoringFilter#passScoreBeforeParsing
   * @see URLMetaScoringFilter#passScoreAfterParsing
   */
  public void passScoreBeforeParsing(Text url, CrawlDatum datum, Content content) {
  }

  /**
   * Takes the metadata, which was lumped inside the content, and replicates it
   * within your parse data.
   * 
   * @see URLMetaScoringFilter#passScoreBeforeParsing
   * @see ScoringFilter#passScoreAfterParsing
   */
  public void passScoreAfterParsing(Text url, Content content, Parse parse) {
  }

  /** Boilerplate */
  public float generatorSortValue(Text url, CrawlDatum datum, float initSort)
      throws ScoringFilterException {
    return initSort;
  }

  /** Boilerplate */
  // Main method for setting boost value before pushing to Solr
  public float indexerScore(Text url, NutchDocument doc, CrawlDatum dbDatum,
      CrawlDatum fetchDatum, Parse parse, Inlinks inlinks, float initScore)
      throws ScoringFilterException {
    open();
    String urlString = url.toString();
    String setBoost = readUrlBoostFromDB(url);

    // If a boost value is set we force this value as boost value when updating
    if (setBoost != "") {
      LOG.info("Boost: " + setBoost + " for " + urlString);
      // Return forced boost value
      close();
      return Float.valueOf(setBoost);
    }
    else {
      // Pass existing automatically calculated score on
      // But first check if any rules exists
      Float factoredBoost = readRuleBasedBoostFactorFromDB(url, parse);
      close();
      return factoredBoost * initScore;
    }
  }

  /** Boilerplate */
  public void initialScore(Text url, CrawlDatum datum)
      throws ScoringFilterException {
    return;
  }

  /** Boilerplate */
  public void injectedScore(Text url, CrawlDatum datum)
      throws ScoringFilterException {
    return;
  }

  public CrawlDatum distributeScoreToOutlinks(Text fromUrl,
    ParseData parseData, Collection<Entry<Text, CrawlDatum>> targets,
    CrawlDatum adjust, int allCount)
    throws ScoringFilterException {
    return adjust;
  }

  public String readUrlBoostFromDB(Text url) {
    try {
      // Statements allow to issue SQL queries to the database
      statement = connect.createStatement();
      // Result set get the result of the SQL query
      resultSet = statement
          .executeQuery("SELECT boost FROM webpage WHERE url = '" + url + "'");
    } catch (Exception e) {
      LOG.error("Exception on executeQuery", e);
    } finally {
      try {
        resultSet.next();
        String boost = resultSet.getString("boost");
        return boost;
      }
      catch (SQLException e) {
        return "";
      }
    }
  }

  public Float readRuleBasedBoostFactorFromDB(Text url, Parse parse) {
    String title = parse.getData().getTitle();
    Float boostFactor = 1.0f;
    try {
      // Statements allow to issue SQL queries to the database
      statement = connect.createStatement();
      // Result set get the result of the SQL query
      // TODO: Test regular expressions
      resultSet = statement
          .executeQuery(
          "SELECT importance_factor FROM metadata_rules WHERE '" + url + "' LIKE CONCAT('%', rule, '%') AND type ='url' AND is_regex <> TRUE " +
          "UNION SELECT importance_factor FROM metadata_rules WHERE '" + url + "' REGEXP rule AND type ='url' AND is_regex = TRUE " +
          "UNION SELECT importance_factor FROM metadata_rules WHERE '" + title + "' LIKE CONCAT('%', rule, '%') AND type ='title' AND is_regex <> TRUE " +
          "UNION SELECT importance_factor FROM metadata_rules WHERE '" + title + "' REGEXP rule AND type ='title' AND is_regex = TRUE"
      );
      while (resultSet.next()) {
        Float thisBoostFactor = resultSet.getFloat("importance_factor");
        boostFactor = boostFactor * thisBoostFactor;
        LOG.info("Rule boost: " + thisBoostFactor + " - new total rule-based boost: " + boostFactor + " url: " + url + " title: " + title);
      }
    } catch (Exception e) {
      LOG.error("Exception on executeQuery (metadata_rules.importance_factor", e);
    }
    return boostFactor;
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
