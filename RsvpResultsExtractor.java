import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

final class RsvpResultsExtractor {
  static final class RsvpQuery {
    public final String name, st;

    public RsvpQuery(final String name, final String st) {
      this.name = name;
      this.st = st;
    }
  }

  static final class RsvpParameters {
    public final String name;
    public final String[] params;

    public RsvpParameters(final String name, final String[] params) {
      this.name = name;
      this.params = params;
    }
  }

  public static void main(final String[] args) {
    final String conStr = "jdbc:oracle:thin:@SMARTR510-SERV1:1521:orcl";
    final String filename = args[1] + "/%s_%s.csv";
    final List<RsvpQuery> queries = new ArrayList<RsvpQuery>();
    final List<RsvpParameters> parameters = new ArrayList<RsvpParameters>();
    Scanner sc = new Scanner(System.in);
    while (sc.hasNextLine()) {
      String line = sc.nextLine();
      if (line.startsWith("--")) {
        final String name = line.substring(2);
        line = "";
        while (sc.hasNextLine()) {
          line += " " + sc.nextLine();
          if (line.endsWith(";")) {
            line = line.substring(0, line.length() - 1);
            break;
          }
        }
        queries.add(new RsvpQuery(name, line));
      }
    }
    try {sc = new Scanner(new File(args[0]));}
    catch (final FileNotFoundException e) {
      throw new RuntimeException(e);
    }
    while (sc.hasNextLine()) {
      final String[] params = sc.nextLine().split(",");
      final String[] ps = new String[params.length - 1];
      for (int i=0; i<ps.length; ++i) ps[i] = params[i + 1];
      parameters.add(new RsvpParameters(params[0], ps));
    }
    try {
      final Connection con = DriverManager.getConnection(conStr, args[2], args[3]);
      final Statement sth = con.createStatement();
      for (RsvpQuery query : queries) {
        System.out.println(query.name);
        for (RsvpParameters param : parameters) {
          System.out.println("  " + param.name);
          BufferedWriter bw = null;
          final String f = String.format(filename, query.name, param.name);
          try {bw = new BufferedWriter(new FileWriter(f));}
          catch (final IOException e) {
            throw new RuntimeException(e);
          }
          final PrintWriter out = new PrintWriter(bw);
          final String st = String.format(query.st, (Object[]) param.params);
          final ResultSet rs = sth.executeQuery(st);
          final int n = rs.getMetaData().getColumnCount();
          while (rs.next()) {
            out.print(rs.getString(1));
            for (int i=2; i<=n; ++i)
              out.print(',' + rs.getString(i));
            out.println();
          }
          try {bw.close();} catch (final IOException e) {
            throw new RuntimeException(e);
          }
        }
      }
    } catch (final SQLException e) {throw new RuntimeException(e);}
  }
}
