/* 
 * Copyright (C) 2016 Stanford University
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package net.ellitron.ldbc.snb.datagen;

import org.docopt.Docopt;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * A utility for merging Person "email" and "speaks" property files into the
 * Person node files in datasets generated by the LDBC SNB Data Generator[1].
 *
 * [1]: git@github.com:ldbc/ldbc_snb_datagen.git
 *
 * @author Jonathan Ellithorpe <jde@cs.stanford.edu>
 */
public class MergePersonProperties {

  private static final String doc =
      "MergePersonProperties: A utility for merging Person email and speaks "
      + "property files into the Person node files in datasets generated by "
      + "the LDBC SNB Data Generator. Output files are placed in the DEST "
      + "directory."
      + "\n"
      + "Usage:\n"
      + "  MergePersonProperties SOURCE DEST\n"
      + "  MergePersonProperties (-h | --help)\n"
      + "  MergePersonProperties --version\n"
      + "\n"
      + "Arguments:\n"
      + "  SOURCE  Directory containing SNB dataset files.\n"
      + "  DEST    Destination directory for output files.\n"
      + "\n"
      + "Options:\n"
      + "  -h --help         Show this screen.\n"
      + "  --version         Show version.\n"
      + "\n";
      
  /**
   * Parse a property file to coalesce multiple properties for a single id into
   * a list of those properties for the id. Used for parsing the email and
   * speaks property files for person nodes, but can be used on any node
   * property file with the same format.
   *
   * @param fileList Array of property files.
   *
   * @return Map from node id to List of property values for the property
   * represented by the parsed file.
   *
   * @throws IOException
   */
  private static Map<String, List<String>> parsePropFiles(File[] fileList)
      throws IOException {
    Map<String, List<String>> propMap = new HashMap<>();
    for (File f : fileList) {
      BufferedReader propFile =
          Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8);

      String line;
      propFile.readLine(); // Skip over the first line (column headers).
      while ((line = propFile.readLine()) != null) {
        String[] lineParts = line.split("\\|");
        String id = lineParts[0];
        String prop = lineParts[1];
        if (propMap.containsKey(id)) {
          propMap.get(id).add(prop);
        } else {
          List<String> list = new ArrayList<>();
          list.add(prop);
          propMap.put(id, list);
        }
      }
      propFile.close();
    }

    return propMap;
  }

  /**
   * Serialize a list of property values into a single String where each element
   * is separated by the given fieldSeparator.
   *
   * @param propList List of property values.
   * @param fieldSeparator Field separator to use between property values.
   *
   * @return Serialized string of property values.
   */
  private static String serializePropertyValueList(List<String> propList, 
      String fieldSeparator) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < propList.size(); i++) {
      // If not first element, start with array separator
      if (i > 0) {
        sb.append(fieldSeparator);
      }

      sb.append(propList.get(i));
    }

    return sb.toString();
  }

  public static void main(String[] args)
      throws FileNotFoundException, IOException, ParseException {
    Map<String, Object> opts =
        new Docopt(doc).withVersion("MergePersonProperties 1.0").parse(args);

    String inputDir = (String) opts.get("SOURCE");
    String outputDir = (String) opts.get("DEST");

    System.out.println(String.format("Processing person properties..."));

    // Prepare email and speaks properties to be added to Person nodes.
    File dir = new File(inputDir);
    File [] fileList = dir.listFiles(new FilenameFilter() {
          @Override
          public boolean accept(File dir, String name) {
            if (name.matches("^person_email_emailaddress_[0-9]+_[0-9]+\\.csv"))
            {
              System.out.println(String.format("Found email property file %s", 
                  name));
              return true; 
            } else {
              return false;
            }
          }
        });

    if (fileList == null || fileList.length == 0) {
      System.out.println("Error: No email address property files found.");
      return;
    }

    Map<String, List<String>> personEmail = parsePropFiles(fileList);

    fileList = dir.listFiles(new FilenameFilter() {
          @Override
          public boolean accept(File dir, String name) {
            if (name.matches("^person_speaks_language_[0-9]+_[0-9]+\\.csv")) {
              System.out.println(
                  String.format("Found language property file %s", name));
              return true; 
            } else {
              return false;
            }
          }
        });

    if (fileList == null || fileList.length == 0) {
      System.out.println("Error: No language property files found.");
      return;
    }

    Map<String, List<String>> personSpeaks = parsePropFiles(fileList);

    fileList = dir.listFiles(new FilenameFilter() {
          @Override
          public boolean accept(File dir, String name) {
            return name.matches(
                "^person_[0-9]+_[0-9]+\\.csv");
          }
        });

    if (fileList == null || fileList.length == 0) {
      System.out.println("Error: No person files found.");
      return;
    } 

    for (File f : fileList) {
      BufferedReader inFile =
          Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8);

      Path path = Paths.get(outputDir + "/" + f.getName());
      BufferedWriter outFile =
          Files.newBufferedWriter(path, StandardCharsets.UTF_8);

      System.out.println(String.format("Processing %s...", f.getName()));

      // Grab the header
      String header = inFile.readLine();

      // Output header to new file and append email and language properties
      outFile.append(header + "|email|language\n");

      String line;
      while ((line = inFile.readLine()) != null) {
        // Append the line as-is
        outFile.append(line);

        // Now lookup the email and language properties for this person.
        String[] colVals = line.split("\\|");
        String id = colVals[0];
        if (personEmail.containsKey(id)) {
          String email = serializePropertyValueList(personEmail.get(id), ";");
          outFile.append("|" + email);
        } else {
          outFile.append("|");
        }

        if (personSpeaks.containsKey(id)) {
          String speaks = serializePropertyValueList(personSpeaks.get(id), ";");
          outFile.append("|" + speaks);
        } else {
          outFile.append("|");
        }

        outFile.append("\n");
      }

      inFile.close();
      outFile.close();
    }
  }
}
