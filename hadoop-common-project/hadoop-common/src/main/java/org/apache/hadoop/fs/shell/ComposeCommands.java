/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.shell;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

/** Various commands for compose files */
@InterfaceAudience.Private
@InterfaceStability.Evolving

class ComposeCommands {  
  public static void registerCommands(CommandFactory factory) {
    factory.addClass(ComposeFromLocal.class, "-composeFromLocal");
    factory.addClass(ComposeFromHDFS.class, "-composeFromHDFS");
  }


  /**
   *  Compose a file in a remote filesystem from existing local files 
   */
  public static class ComposeFromLocal extends CommandWithDestination {
    public static final String NAME = "composeFromLocal";
    public static final String USAGE = "[-f] [-p] <localsrc> ... <dst>";
    public static final String DESCRIPTION =
    "Compose files from the local file system\n" +
    "into fs. Composing fails if the file already\n" +
    "exists, unless the -f flag is given. Passing\n" +
    "-p preserves access and modification times,\n" +
    "ownership and the mode. Passing -f overwrites\n" +
    "the destination if it already exists.\n";

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf = new CommandFormat(1, Integer.MAX_VALUE, "f", "p");
      cf.parse(args);
      setOverwrite(cf.getOpt("f"));
      setPreserve(cf.getOpt("p"));
      getRemoteDestination(args);
      // should have a -r option
      setRecursive(true);
    }

    // commands operating on local paths have no need for glob expansion
    @Override
    protected List<PathData> expandArgument(String arg) throws IOException {
      try {
        List<PathData> items = new LinkedList<PathData>();
        items.add(new PathData(new URI(arg), getConf()));
        return items;
      } catch (URISyntaxException e) {
        throw new IOException("unexpected URISyntaxException", e);
      }
    }

    @Override
    protected void processArguments(LinkedList<PathData> args)
    throws IOException {
      // NOTE: this logic should be better, mimics previous implementation
      if (args.size() == 1 && args.get(0).toString().equals("-")) {
        copyStreamToTarget(System.in, getTargetPath(args.get(0)));
        return;
      }
      super.processArguments(args);
    }
  }

  public static class ComposeFromHDFS extends CommandWithDestination {
    public static final String NAME = "composeFromHDFS";
    public static final String USAGE = "[-f] [-p] <src> ... <dst>";
    public static final String DESCRIPTION = 
    "Compose files from the another federated HDFS\n" +
    "Composing fails if the file already\n" +
    "exists, unless the -f flag is given. Passing\n" +
    "-p preserves access and modification times,\n" +
    "ownership and the mode. Passing -f overwrites\n" +
    "the destination if it already exists.\n";

    @Override
    protected void processOptions(LinkedList<String> args) throws IOException {
      CommandFormat cf = new CommandFormat(1, Integer.MAX_VALUE, "f", "p");
      cf.parse(args);
      setOverwrite(cf.getOpt("f"));
      setPreserve(cf.getOpt("p"));
      getRemoteDestination(args);
      // should have a -r option
      setRecursive(true);
    }

    @Override
    protected List<PathData> expandArgument(String arg) throws IOException {
      List<PathData> items = new LinkedList<PathData>();
      try {
        items.add(new PathData(new URI(arg), getConf()));
      } catch (URISyntaxException e) {
        if (Path.WINDOWS) {
          // Unlike URI, PathData knows how to parse Windows drive-letter paths.
          items.add(new PathData(arg, getConf()));
        } else {
          throw new IOException("unexpected URISyntaxException", e);
        }
      }
      return items;
    }

    @Override
    protected void processArguments(LinkedList<PathData> args)
    throws IOException {
        // NOTE: this logic should be better, mimics previous implementation
      if (args.size() == 1 && args.get(0).toString().equals("-")) {
        copyStreamToTarget(System.in, getTargetPath(args.get(0)));
        return;
      }
      super.processArguments(args);
    }

    @Override
    protected void processPath(PathData src, PathData dst) throws IOException {
      System.out.println("[compose] processPath: " + src.fs + ", "+ dst.fs);
      if (src.stat.isSymlink()) {
        // TODO: remove when FileContext is supported, this needs to either
        // copy the symlink or deref the symlink
        throw new PathOperationException(src.toString());        
      } else if (src.stat.isFile()) {
        composeFileToTarget(src, dst);
      } else if (src.stat.isDirectory() && !isRecursive()) {
        throw new PathIsDirectoryException(src.toString());
      }
    }

  }
}
