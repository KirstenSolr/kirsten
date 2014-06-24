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

package kirsten;

import java.io.*;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Map.Entry;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Scanner; 
import javax.servlet.*;
import javax.servlet.ServletContext;
import javax.servlet.http.*;
import javax.servlet.http.HttpServletRequest;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathExpressionException;
import com.mysql.jdbc.Driver;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * Main class
 */
public class Service extends HttpServlet {
  private Connection connect;
  private PreparedStatement pstmt;
  private XPath xpath;
  private Logger LOG;
  private DocumentBuilderFactory dbf;
  private DocumentBuilder db;
  private Document doc;
  private HttpServletRequest request;
  private String message;
  private String success;
  private String kirstenDbUrl;
  private String kirstenDbDbname;
  private String kirstenDbUsername;
  private String kirstenDbPassword;
  private String url = null;
  private Float boostFloat;
  private String boost;
  private ServletContext context;

  public void init() throws ServletException
  {
    LOG = LoggerFactory.getLogger(this.getClass());
    try {
      loadConf();
    } catch (Exception e){
      LOG.error("Error loading configuration for web service", e);
      success = "0";
    }
  }

  public void doGet(HttpServletRequest request,
                    HttpServletResponse response)
            throws ServletException, IOException
  {
    success = "1";
    message = "";
    
    try {
      url = request.getParameterValues("url")[0];
      url = URLDecoder.decode(url, "UTF-8");
    } catch (Exception e) {
      success = "0";
      message += "Missing or malformed URL. ";
    }
    try {
      boost = request.getParameterValues("boost")[0];
      boostFloat = Float.parseFloat(boost);
      if (boostFloat > 1) {
        success = "0";
        message += "Boost should be positive value between 0.00001 and 1.0. ";        
      }
    } catch (Exception e) {
      boost = null;
    }
    
    if (success == "1") {
      updateDB();
    
      try {
        triggerUpdate();
      } catch (Exception e) {
        success = "0";
        message += "Error initiating update script. ";
        LOG.error("Error starting update script", e);
      }
    }
    
    // Set response content type
    response.setContentType("text/xml");

    // Actual logic goes here.
    PrintWriter out = response.getWriter();
    out.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
    out.println("<response>");
    out.println("  <success>" + success + "</success>");
    out.println("  <message>" + message + "</message>");
    out.println("</response>");
  }
  
  public void destroy()
  {
  }

  public String getNutchConf(Document document, String tagName) 
    throws XPathExpressionException
  {
    Node dbNode = (Node) xpath.evaluate("/configuration/property[name=\"" + tagName + "\"]/value/text()", document, XPathConstants.NODE);
    return dbNode.getTextContent();
  }
  
  public void loadConf()
    throws ParserConfigurationException, SAXException, IOException, XPathExpressionException
  {
    xpath = XPathFactory.newInstance().newXPath();
    dbf = DocumentBuilderFactory.newInstance();
    db = dbf.newDocumentBuilder();
    
    File file = new File(getServletContext().getInitParameter("nutchConfig")); 

    doc = db.parse(file);
     
    doc.getDocumentElement().normalize();
    
    // Open connection
    try {
      kirstenDbUrl = getNutchConf(doc, "kirsten.db.url");
      kirstenDbDbname = getNutchConf(doc, "kirsten.db.dbname");
      kirstenDbUsername = getNutchConf(doc, "kirsten.db.username");
      kirstenDbPassword = getNutchConf(doc, "kirsten.db.password");
    }
    catch (Exception e) {
      LOG.info("Error parsing configuration.", e);
    }
  }
  
  public void updateDB() {
    try {
      Class.forName("com.mysql.jdbc.Driver");
      // Setup the connection with the DB
      connect = DriverManager
        .getConnection(kirstenDbUrl + "/" + kirstenDbDbname, kirstenDbUsername, kirstenDbPassword);
    } catch (Exception e) {
      LOG.error("Error connecting to DB", e);
    }
    try {
      String query = 
        "INSERT INTO webpage (url, boost) VALUES (?, ?) " +
        "ON DUPLICATE KEY UPDATE boost = ?;";
      pstmt = connect.prepareStatement(query);
      pstmt.setString(1, url);
      pstmt.setString(2, boost);
      pstmt.setString(3, boost);
      pstmt.executeUpdate();
    } catch (Exception e) {
      LOG.error("Exception on executeQuery", e);
      success = "0";
      message += "Error updating DB. ";
    } finally {
      try {
        connect.close();
        pstmt.close();
      } catch (Exception e) {
        LOG.error("Error closing DB connection", e);
        success = "0";
        message += "Error closing DB connection. ";
      }
    }
  }

  public void triggerUpdate() 
    throws java.io.IOException, java.lang.InterruptedException
  {
    // Create ProcessBuilder instance for UNIX command ls -l
    java.lang.ProcessBuilder processBuilder = new java.lang.ProcessBuilder(
      "sh", 
      getServletContext().getInitParameter("nutchScript"),
      url,
      getServletContext().getInitParameter("nutchData")
    );
    // Create an environment (shell variables)
    java.util.Map env = processBuilder.environment();
    env.clear();
    processBuilder.directory(new java.io.File(getServletContext().getInitParameter("nutchData")));
    // Start new process
    java.lang.Process p = processBuilder.start();
    LOG.info("Background process initiated");
  }
}