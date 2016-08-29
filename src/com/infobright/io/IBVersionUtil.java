package com.infobright.io;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Query Infobright and return the version string.
 * 
 */
class IBVersionUtil {

    /**
     * MySQL variable name containing the IB release number
     */
    private static final String VAR_IB_RELEASE = "version_comment";

    /**
     * IB first major release number that supports LOAD DATA LOCAL INFILE
     */
    private static final int VER_MAJOR = 3;

    /**
     * IB first minor release number that supports LOAD DATA LOCAL INFILE
     */
    private static final int VER_MINOR = 5;

    /**
     * Regex used to match the version number
     */
    private static final String VER_REGEX = ".*(?:IEE_|IB_)(\\d+\\.\\d+\\.\\d+)_.*";

    /**
     * Precompiled Pattern object with VER_REGEX
     */
    private static final Pattern versionPattern = Pattern.compile(VER_REGEX);

    private final Connection connection;

    public IBVersionUtil(Connection connection) {
        this.connection = connection;
    }

    /**
     * Query the IB version number from the JDBC connection, parse it and determine if this version supports LOAD DATA
     * LOCAL INFILE.
     * 
     * @return
     * @throws SQLException
     */
    public boolean isSupportsLocalInfile() throws SQLException {
        String versionComment = this.queryVariable(VAR_IB_RELEASE);
        String versionNumber = null;

        // Use regex matcher and get version number
        Matcher matcher = versionPattern.matcher(versionComment);

        if (matcher.matches()) {
            versionNumber = matcher.group(1);
        } else {
            // No match -> invalid version number
            throw new RuntimeException("Invalid version_comment '" + versionComment + "'");
        }

        // versionNumber is something like "3.4.2" or "3.5.0"
        String[] sDigits = versionNumber.split("\\.");
        if (sDigits.length < 2) {
            throw new RuntimeException("Invalid version_comment '" + versionComment + "'");
        }
        int[] digits = new int[sDigits.length];
        for (int i = 0; i < sDigits.length; i++) {
            digits[i] = Integer.parseInt(sDigits[i]);
        }
        return ((digits[0] > VER_MAJOR) ||
            (digits[0] == VER_MAJOR && digits[1] >= VER_MINOR));
    }

    private String queryVariable(String variableName) throws SQLException {
        String SQL = "show variables like '" + variableName + "'";
        String val = null;
        ResultSet rs = null;
        Statement stmt = connection.createStatement();
        try {
            stmt.executeQuery(SQL);
            rs = stmt.getResultSet();
            int numRows = 0;
            while (rs.next()) {
                numRows++;
                val = rs.getObject(2).toString();
                break;
            }
            if (numRows == 0) {
                throw new RuntimeException("Failed to retrieve variable '" + variableName + "'");
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs != null) {
                rs.close();
            }
            stmt.close();
        }
        return val;
    }
}