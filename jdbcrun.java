package com.ef.JDBCrun;

import java.sql.* ;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.function.BooleanSupplier;

/**
 * Created by Justin on 6/7/16.
 */
public class jdbcrun {
    // JDBC driver name and database URL
    private final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private final String DB_URL = "jdbc:mysql://localhost:3306/crossbird?useUnicode=true&characterEncoding=UTF-8";

    //  Database credentials
    private final String USER = "root";
    private final String PASS = "";

    // Headers
    private String mTableName = "";
    private String[] mIndeces = {
            "`tag_id`",
            "`status_id`",
            "`company_id`",
            "`publish_at`",
            "`freezed`"
    };
    private String[] mCurrentOrderList = new String[5];

    // Command Switch
    private final boolean COMMAND_POPULATE_TEST_DATA = false;
    private final boolean COMMAND_RUN_QUERY_TEST = false;
    private final boolean COMMAND_RUN_BATCH_INSERT = true;

    // Query Options
    private final boolean COMMAND_RUN_OLD_QUERY = false;
    private final boolean COMMAND_RUN_EXPLAIN_QUERY_TEST = false;
    private final boolean COMMAND_RUN_QUERY_FAST = false;

    // Print options
    private final boolean PRINT_DEBUG_SQLQUERY = false;

    private final int TIMES_TO_TEST = 100;
    private final int NUMBER_OF_ENTRIES_TO_POPULATE = 3000000;

    private double mLowestAverage = 1;
    private String mWinningQuery = "";
    private String mCurrentQuery = "";
    private String mCurrentOrder = "";
    private int mFastestTest = 0;
    private int mtestCount = 0;
    private int mPopulateCount = 0;

    private List<String> mTestList;

    public static void main(String[] args) {
        jdbcrun program = new jdbcrun();
        program.start();
    }

    private void start(){
        mTestList = new ArrayList<>();

        if (COMMAND_RUN_OLD_QUERY && COMMAND_RUN_QUERY_TEST) {
            mTableName = "blog_v2";
        } else {
            mTableName = "index_article_mine";
        }

        if (COMMAND_POPULATE_TEST_DATA)
//            for (int i = 0; i < NUMBER_OF_ENTRIES_TO_POPULATE; i++) {
                populateTestData();
//            }

        if (COMMAND_RUN_QUERY_TEST || COMMAND_RUN_EXPLAIN_QUERY_TEST) {
            if (COMMAND_RUN_OLD_QUERY) {
                for (int i = 0; i < TIMES_TO_TEST; i++) {
                    runSelectQueryTest();
                }
            } else {
                runIndexTest(generateIndices(), 0);
                printFinalReport();
            }
        }

        if (COMMAND_RUN_BATCH_INSERT) { batchInsert(); }
    }

    private void populateTestData () {
        mPopulateCount++;
        if ((mPopulateCount%1000) == 0) { System.out.println("Entry: " + NumberFormat.getIntegerInstance().format(mPopulateCount)); }

        Connection conn = null;
        Statement stmt = null;
        try{
            Class.forName("com.mysql.jdbc.Driver");

            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            stmt = conn.createStatement();
            String sql;
//            sql = "insert into " + mTableName + " (id, status_id, name, title, created_at, updated_at, public_at, close_at, start_at, end_at, authors, contributers, rights, source, description, mimetype, content, content_mobile, extension, locale, timezone, language, url, image, latitude, longitude, vertical_accuracy, horizontal_accuracy, geohash, location_id, memo, externalid, properties, rights_owner_read, rights_owner_write, rights_group_read, rights_group_write, rights_other_read, rights_other_write, aux_string_1, aux_string_2, aux_string_3, aux_string_4, aux_string_5, aux_string_6, aux_string_7, aux_string_8, aux_string_9, aux_string_10, aux_string_11, aux_string_12, aux_string_13, aux_string_14, aux_string_15, aux_string_16, aux_string_17, aux_string_18, aux_string_19, aux_string_20, aux_long_1, aux_long_2, aux_long_3, aux_long_4, aux_long_5, aux_long_6, aux_long_7, aux_long_8, aux_long_9, aux_long_10, aux_double_1, aux_double_2, aux_double_3, aux_double_4, aux_double_5, aux_double_6, aux_double_7, aux_double_8, aux_double_9, aux_double_10, aux_text_1, aux_text_2, aux_text_3, aux_text_4, aux_text_5, aux_text_6, aux_text_7, aux_text_8, aux_text_9, aux_text_10, class_id, created_by, updated_by, owner_id, group_id, privilege_id, shop_id, brand_id, company_id, freezed, shortid, v1id, version, context_id, image_primary_id, link_primary_id, publish_at, display_level, name_en, name_locale, revision, predicate, context_appid, tag_id, association_id) \n" +
//                    "values ('autotest-autotest-0-blog-edc2f19d-4710-405c-8977-"+generateID()+"','"+generateRandomStatusID()+"','autotest-autotest-0-blog-edc2f19d-4710-405c-8977-000000000001','blog autotest title','2014-11-14 05:55:35','2014-11-14 05:55:35','2016-04-27 03:59:27',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'test/html','blog autotest contents',NULL,NULL,'ja_JP','JST',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'1','1','1','0','0','0',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'system-system-0-class-apparelcloud.blog','pal-palshop-1409632435406-user-40880fc2-6647-448d-95b9-35a921a940a8','pal-palshop-1409632435406-user-40880fc2-6647-448d-95b9-35a921a940a8','pal-palshop-1409632435406-user-40880fc2-6647-448d-95b9-35a921a940a8','pal-palshop-1409632435406-user-40880fc2-6647-448d-95b9-35a921a940a8',NULL,'4481','246','"+generateRandomCompanyID()+"','"+generateRandomFreezed()+"',NULL,NULL,NULL,NULL,NULL,NULL,'2014-11-14 05:55:35',NULL,NULL,NULL,NULL,'birthday-month','PALShopApp',"+generateTagID()+",'autotest-autotest-0-blog-edc2f19d-4710-405c-8977-"+generateID()+"')";
            sql = "insert into " + mTableName + " (resource_id,resource_kind,association_kind_id,association_target_id,appid,company_id,brand_id,shop_id,publish_at,created_at,updated_at,public_at,close_at,start_at,end_at,pageview,pageview_yesterday,followed_count,sales_total) \n" +
                    "VALUES ('autotest-autotest-0-blog-edc2f19d-4710-405c-8977-"+generateID()+"','system-system-0-class-association', 'system-system-0-tag-tag', 'abiste-abiste-1405320569378-tag-41d8cc1c-0e8c-46da-a3db-58c7bf32a213', 'console', '159', '526', '44148', '20140714171500', '20140715021837', '20140715021837', '20140714171500', '20981231150000', NULL, NULL, NULL, NULL, NULL, NULL)";
            stmt.executeUpdate(sql);

            stmt.close();
            conn.close();
        }catch(SQLException se){
            //Handle errors for JDBC
            se.printStackTrace();
        }catch(Exception e){
            //Handle errors for Class.forName
            e.printStackTrace();
        }finally {
            //finally block used to close resources
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }// nothing we can do
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }//end finally try
        }
    }

    private String[] getDates() {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date date = new Date(System.currentTimeMillis() - ( 60 * 60 * 1000));
        String previousTime = sdf.format(date);

        java.util.Date dt = new java.util.Date();
        String currentTime = sdf.format(dt);


        return new String[] {previousTime, currentTime};
    }

    private void batchInsert() {
        String start = getDates()[0];
        String end = getDates()[1];

        Connection conn = null;
        Statement stmt = null;
        try{
            Class.forName("com.mysql.jdbc.Driver");

            conn = DriverManager.getConnection(DB_URL,USER,PASS);

            stmt = conn.createStatement();
            String sql;
            sql = "INSERT INTO index_article_mine\n" +
                    "SELECT\n" +
                    "b.id as resource_id, assoc.class_id as resource_kind, assoc.kind_id as association_kind_id, assoc.right_id as assocation_target_id, b.context_appid as appid, b.company_id, b.brand_id, b.shop_id, b.publish_at, b.created_at, b.updated_at, b.public_at, b.close_at, b.start_at, b.end_at, null, null, null, null\n" +
                    "FROM\n" +
                    " crossbird.blog_v2 b\n" +
                    "LEFT JOIN crossbird.association_v2 assoc\n" +
                    "ON b.id = assoc.left_id\n" +
                    "WHERE\n" +
                    "  b.freezed <> 2 \n" +
                    "  AND assoc.freezed = 0\n" +
                    "--   and '2016-04-27 03:59:27' between coalesce(b.public_at,'2016-04-27 03:59:27') and coalesce(b.close_at,'2016-04-27 03:59:27')\n" +
                    "  and b.id in (\n" +
                    "    Select\n" +
                    "      assoc.left_id as assoc_left_id\n" +
                    "     From\n" +
                    "      association_v2 assoc\n" +
                    "     Where\n" +
                    "      assoc.freezed = 0\n" +
                    "      and assoc.left_id = b.id\n" +
                    "      and assoc.right_id in (\n" +
                    "        Select\n" +
                    "\t      t.id as tag_id\n" +
                    "        From\n" +
                    "          crossbird.tag_v2 t\n" +
                    "\t\tWhere\n" +
                    "        t.freezed = 0\n" +
                    "        and t.id = assoc.right_id\n" +
                    "        )\n" +
                    "  )\n" +
                    "-- LIMIT 10000\n" +
                    ";";
            System.out.println("Insert Started");
            stmt.executeUpdate(sql);
            System.out.println("End");

            stmt.close();
            conn.close();
        }catch(SQLException se){
            se.printStackTrace();
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try{
                if(stmt!=null)
                    stmt.close();
            }catch(SQLException se2){
            }
            try{
                if(conn!=null)
                    conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
    }

    private void joinAssocToBlog() {
        Connection conn = null;
        Statement stmt0 = null;
        Statement stmt = null;
        Statement stmt2 = null;
        Statement stmt3 = null;

        int blogCount = 0;
        try {
            //STEP 2: Register JDBC driver
            Class.forName("com.mysql.jdbc.Driver");

            //STEP 3: Open a connection
            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            //STEP 4: Execute a query
            System.out.println("Creating statement...");

            //~~~~~~~ Find Blog Size
            stmt0 = conn.createStatement();
            String sql0 = "SELECT\n" +
                    "Count(id)\n" +
                    "FROM\n" +
                    "blog_v2";
            ResultSet rsBlog0 = stmt0.executeQuery(sql0);
            while (rsBlog0.next()) {
                blogCount = Integer.parseInt(rsBlog0.getString(1));
            }
            stmt0.close();

            //~~~~~~~ Query Batch
            int batchSize = 500;
            int batchCount = (blogCount / batchSize) + 1;

            String blogID;
            for (int x = 0; x < batchCount; x++) {
                System.out.println("BATCH " + x + "/" + batchCount);
                stmt = conn.createStatement();
                String sql = "SELECT\n" +
                        "*\n" +
                        "FROM\n" +
                        "blog_v2\n" +
                        "LIMIT " + batchSize + " OFFSET " + (batchSize * x);
                ResultSet rsBlog = stmt.executeQuery(sql);

                ResultSetMetaData rsmdBlog = rsBlog.getMetaData();
                int columnCountBlog = rsmdBlog.getColumnCount();

                //STEP 5: Extract data from result set
                while (rsBlog.next()) {
                    blogID = rsBlog.getString(1);

                    stmt2 = conn.createStatement();
                    String sql2 = "SELECT\n" +
                            "association_v3.right_id as tag_id,\n" +
                            "association_v3.id as association_id\n" +
                            "FROM\n" +
                            "association_v3\n" +
                            "WHERE\n" +
                            "association_v3.kind_id = 'system-system-0-tag-tag'\n" +
                            "AND association_v3.freezed = 0\n"+
                            "AND association_v3.left_id = " + "'" + blogID + "'";
                    ResultSet rsAssoc = stmt2.executeQuery(sql2);

                    ResultSetMetaData rsmdAssoc = rsAssoc.getMetaData();
                    int columnCountAssoc = rsmdAssoc.getColumnCount();

                    boolean hasrsAssoc = false;

                    while (rsAssoc.next()) {
                        hasrsAssoc = true;

                        stmt3 = conn.createStatement();
                        StringBuilder sb = new StringBuilder();
                        sb.append("insert into blog_v3 (");
                        for (int i = 1; i <= columnCountBlog; i++) {
                            sb.append(rsmdBlog.getColumnLabel(i)).append(", ");
                        }
                        for (int i = 1; i <= columnCountAssoc; i++) {
                            if (i == columnCountAssoc) {
                                sb.append(rsmdAssoc.getColumnLabel(i));
                            } else {
                                sb.append(rsmdAssoc.getColumnLabel(i)).append(", ");
                            }
                        }
                        sb.append(") values (");

                        String value;
                        for (int i = 1; i <= columnCountBlog; i++) {
                            value = rsBlog.getString(i);
                            if (value == null) {
                                sb.append("null").append(", ");
                            }  else {
                                sb.append("'").append(value.replace("'", "''")).append("', ");
                            }
                        }
                        for (int i = 1; i <= columnCountAssoc; i++) {
                            value = rsAssoc.getString(i);
                            if (value == null) {
                                sb.append("null").append(", ");
                            } else if (i == columnCountAssoc){
                                sb.append("'").append(value.replace("'", "''")).append("'");
                            } else {
                                sb.append("'").append(value.replace("'", "''")).append("', ");
                            }
                        }
                        sb.append(")");

                        String query = sb.toString();
                        try {
                            stmt3.executeUpdate(sb.toString());
                        } catch (SQLException e)  {
                            System.out.println(query);
                        }

                        stmt3.close();
                    }

                    if (hasrsAssoc == false) {

                        stmt3 = conn.createStatement();
                        StringBuilder sb = new StringBuilder();
                        sb.append("insert into blog_v3 (");
                        for (int i = 1; i <= columnCountBlog; i++) {
                            sb.append(rsmdBlog.getColumnLabel(i)).append(", ");
                        }
                        for (int i = 1; i <= columnCountAssoc; i++) {
                            if (i == columnCountAssoc) {
                                sb.append(rsmdAssoc.getColumnLabel(i));
                            } else {
                                sb.append(rsmdAssoc.getColumnLabel(i)).append(", ");
                            }
                        }
                        sb.append(") values (");

                        String value;
                        for (int i = 1; i <= columnCountBlog; i++) {
                            value = rsBlog.getString(i);
                            if (value == null) {
                                sb.append("null").append(", ");
                            }  else {
                                sb.append("'").append(value.replace("'", "''")).append("', ");
                            }
                        }
                        for (int i = 1; i <= columnCountAssoc; i++) {
                            if (i == columnCountAssoc){
                                sb.append("''");
                            } else {
                                sb.append("'', ");
                            }
                        }
                        sb.append(")");

                        String query = sb.toString();
                        try {
                            stmt3.executeUpdate(sb.toString());
                        } catch (SQLException e)  {
                            System.out.println(query);
                        }

                        stmt3.close();
                    }

                    rsAssoc.close();
                }
                rsBlog.close();
                stmt.close();
                stmt2.close();
            }

            System.out.println("end");


            //STEP 6: Clean-up environment
            conn.close();
        } catch (SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            //finally block used to close resources
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }// nothing we can do
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }//end finally try
        }//end try
    }

    private void runIndexTest(int[] a, int k) {
        if (k == a.length)
        {
            runIndexReassign(a);
        }
        else
        {
            for (int i = k; i < a.length; i++)
            {
                int temp = a[k];
                a[k] = a[i];
                a[i] = temp;

                runIndexTest(a, k + 1);

                temp = a[k];
                a[k] = a[i];
                a[i] = temp;
            }
        }
    }

    private String getSelectQuery() {
        String sql = "";
        if (COMMAND_RUN_EXPLAIN_QUERY_TEST) {
            sql = "Explain\n";
        }
        if (COMMAND_RUN_OLD_QUERY) {
            sql = sql + "Select\n" +
                    "  b.id\n" +
                    "From\n" +
                    "  blog_v2 b\n" +
                    "Where\n" +
                    "  b.freezed = 0\n" +
                    "  and b.status_id = 1\n" +
                    "  and '2016-04-27 03:59:27' between coalesce(b.public_at,'2016-04-27 03:59:27') and coalesce(b.close_at,'2016-04-27 03:59:27')\n" +
                    "  and b.company_id in (115)\n" +
                    "  and b.id in (\n" +
                    "    Select\n" +
                    "      assoc.left_id as assoc_left_id\n" +
                    "     From\n" +
                    "      association_v2 assoc\n" +
                    "     Where\n" +
                    "      assoc.freezed = 0\n" +
                    "      and assoc.kind_id = 'system-system-0-tag-tag'\n" +
                    "      and assoc.left_id = b.id\n" +
                    "      and assoc.right_id in (\n" +
                    "\t\t'abiste-abiste-1405320597413-tag-86ecee7b-e4ae-4e45-bd16-32045eeda626',\n" +
                    "\t\t'apparelweb-staffsnapteam-1410233030569-tag-de83c6e4-0841-4c03-a6d9-fff3cb8f23e6',\n" +
                    "\t\t'bals-francfranc-1403238069195-tag-9994cd8f-d83c-47bb-ad40-9b404e505c33',\n" +
                    "\t\t'onward-paulsmith-1424935437157-tag-dbadc618-10c0-4027-9195-5fb0fad985ff',\n" +
                    "\t\t'pal-threecoins-1435729378547-tag-ac74ae32-e2a6-4719-b5f7-44b9b13dac76',\n" +
                    "\t\t'pal-threecoins-1435729793876-tag-a6cc9784-d0f5-4176-b7d3-404ff50b75f9',\n" +
                    "\t\t'system-system-1402319141519-tag-e3051f04-d7c8-4765-85c0-1662a0af3e74',\n" +
                    "\t\t'system-system-1404735591059-tag-de479456-89bd-4efb-9fd9-2c2d75aae331',\n" +
                    "\t\t'system-system-1414665133304-tag-21b1041a-d55c-4830-a8f3-4b5f9361ab1f',\n" +
                    "\t\t'system-system-1421331225217-tag-058d7ab3-e87c-4473-b436-afbd48143def',\n" +
                    "\t\t'system-system-1433290282064-tag-c9c33624-71a3-4685-a199-44ec9c1ec3b3',\n" +
                    "\t\t'system-system-1434018212337-tag-b0fe14e2-d0d2-40e8-a325-da288d3ba64e',\n" +
                    "\t\t'system-system-1436969420905-tag-28ebb84e-f4bf-4c7f-a62e-eaffe13bbe4b',\n" +
                    "\t\t'system-system-1440064428407-tag-00cb0a2c-c509-4345-af49-e2d0176f8fd3',\n" +
                    "\t\t'system-system-1441272142422-tag-d1eeddfc-bed9-4ff8-8887-898e3d262186',\n" +
                    "\t\t'system-system-1441954221138-tag-4522bd8e-2bac-4d76-a460-87a9e5867a77',\n" +
                    "\t\t'system-system-1442487179175-tag-8e8612c4-94b2-4e8a-9b3d-0a1a96d58e7d',\n" +
                    "\t\t'system-system-1445567983204-tag-915d7384-c853-469a-943a-e4ceb219af71',\n" +
                    "\t\t'system-system-1457672438043-tag-d88f2173-e7c7-40c0-b11e-4e32f5747824',\n" +
                    "\t\t'system-system-1457673702495-tag-5ece6b3c-c1f2-4b30-b802-bc6ba5bde1e1',\n" +
                    "\t\t'system-system-1457955684034-tag-0c8bd2ba-cec1-4298-9210-65b1d6fc2772'\n" +
                    "\t)\n" +
//                    "\tGROUP BY assoc_left_id\n" +
                    "  )\n" +
                    "  and b.id not in (\n" +
                    "    Select\n" +
                    "      assoc2.left_id as assoc2_left_id\n" +
                    "    From\n" +
                    "       association_v2 assoc2\n" +
                    "    Where\n" +
                    "    assoc2.kind_id = 'system-system-0-tag-tag'\n" +
                    "    and assoc2.freezed = 0\n" +
                    "    and assoc2.right_id in ('pal-threecoins-1429429326818-tag-b2ca433a-1865-421e-987a-f8e68ff5f02d')\n" +
                    "  )\n" +
//                    "GROUP BY b.id\n" +
                    "Order By\n" +
                    "  b.publish_at Desc\n" +
                    "limit 1000 offset 0";
        } else {
            sql = sql + "Select\n" +
                    "  " + mTableName + ".id\n" +
                    "From\n" +
                    "  " + mTableName + "\n" +
                    "Where\n" +
                    "  " + mTableName + ".freezed = 0\n" +
                    "  and " + mTableName + ".status_id = 1\n" +
                    "  and '2016-04-27 03:59:27' between coalesce(" + mTableName + ".public_at,'2016-04-27 03:59:27') and coalesce(" + mTableName + ".close_at,'2016-04-27 03:59:27')\n" +
                    "  and " + mTableName + ".company_id in (115)\n" +
                    "  and " + mTableName + ".tag_id in (\n" +
                    "\t\t'abiste-abiste-1405320597413-tag-86ecee7b-e4ae-4e45-bd16-32045eeda626',\n" +
                    "\t\t'apparelweb-staffsnapteam-1410233030569-tag-de83c6e4-0841-4c03-a6d9-fff3cb8f23e6',\n" +
                    "\t\t'bals-francfranc-1403238069195-tag-9994cd8f-d83c-47bb-ad40-9b404e505c33',\n" +
                    "\t\t'onward-paulsmith-1424935437157-tag-dbadc618-10c0-4027-9195-5fb0fad985ff',\n" +
                    "\t\t'pal-threecoins-1435729378547-tag-ac74ae32-e2a6-4719-b5f7-44b9b13dac76',\n" +
                    "\t\t'pal-threecoins-1435729793876-tag-a6cc9784-d0f5-4176-b7d3-404ff50b75f9',\n" +
                    "\t\t'system-system-1402319141519-tag-e3051f04-d7c8-4765-85c0-1662a0af3e74',\n" +
                    "\t\t'system-system-1404735591059-tag-de479456-89bd-4efb-9fd9-2c2d75aae331',\n" +
                    "\t\t'system-system-1414665133304-tag-21b1041a-d55c-4830-a8f3-4b5f9361ab1f',\n" +
                    "\t\t'system-system-1421331225217-tag-058d7ab3-e87c-4473-b436-afbd48143def',\n" +
                    "\t\t'system-system-1433290282064-tag-c9c33624-71a3-4685-a199-44ec9c1ec3b3',\n" +
                    "\t\t'system-system-1434018212337-tag-b0fe14e2-d0d2-40e8-a325-da288d3ba64e',\n" +
                    "\t\t'system-system-1436969420905-tag-28ebb84e-f4bf-4c7f-a62e-eaffe13bbe4b',\n" +
                    "\t\t'system-system-1440064428407-tag-00cb0a2c-c509-4345-af49-e2d0176f8fd3',\n" +
                    "\t\t'system-system-1441272142422-tag-d1eeddfc-bed9-4ff8-8887-898e3d262186',\n" +
                    "\t\t'system-system-1441954221138-tag-4522bd8e-2bac-4d76-a460-87a9e5867a77',\n" +
                    "\t\t'system-system-1442487179175-tag-8e8612c4-94b2-4e8a-9b3d-0a1a96d58e7d',\n" +
                    "\t\t'system-system-1445567983204-tag-915d7384-c853-469a-943a-e4ceb219af71',\n" +
                    "\t\t'system-system-1457672438043-tag-d88f2173-e7c7-40c0-b11e-4e32f5747824',\n" +
                    "\t\t'system-system-1457673702495-tag-5ece6b3c-c1f2-4b30-b802-bc6ba5bde1e1',\n" +
                    "\t\t'system-system-1457955684034-tag-0c8bd2ba-cec1-4298-9210-65b1d6fc2772'\n" +
                    "\t)\n" +
                    "  and " + mTableName + ".tag_id not in ('pal-threecoins-1429429326818-tag-b2ca433a-1865-421e-987a-f8e68ff5f02d')\n" +
//                    "Group By\n" +
//                    "  " + mTableName + ".id\n" +
                    "Order By\n" +
                    "  " + mTableName + ".id Desc\n" +
                    "limit 1000 offset 0";
        }
        return sql;
    }

    private void runSelectQueryTest() {
//        System.out.println("Running Select Query");
        Connection conn = null;
        Statement stmt = null;

        try {
            //STEP 2: Register JDBC driver
            Class.forName("com.mysql.jdbc.Driver");

            //STEP 3: Open a connection
//            System.out.println("Connecting to database...");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            //STEP 4: Execute a query
//            System.out.println("Creating statement with Runtime...");
            stmt = conn.createStatement();

            String sql = getSelectQuery();

            double average = 0;
            if (COMMAND_RUN_EXPLAIN_QUERY_TEST) {
//                System.out.println("Executing Query");
                ResultSet rs = stmt.executeQuery(sql);

                ResultSetMetaData rsmdBlog = rs.getMetaData();
                int countBlogv4 = rsmdBlog.getColumnCount();

                while(rs.next()){
                    System.out.print("TEST: " + mtestCount + " - ");
                    for (int i = 1; i <= countBlogv4; i++) {
                        System.out.print(rs.getString(i) + "\t");
                    }
                    System.out.println();
                }


                rs.close();
            } else if (COMMAND_RUN_QUERY_TEST) {
                for (int i = 0; i < TIMES_TO_TEST; i++) {
                    long startDate = System.nanoTime();
                    ResultSet rs = stmt.executeQuery(sql);
                    long elapsedTime = System.nanoTime() - startDate;
                    double seconds = ((double)elapsedTime / 1000000000);
                    average += seconds;
                    rs.close();
                }
            }

            average = average/ TIMES_TO_TEST;
            printReport(average);
            if (average < mLowestAverage) { mLowestAverage = average; mFastestTest = mtestCount; mWinningQuery = mCurrentQuery; }

            stmt.close();
            conn.close();
        } catch (SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            //finally block used to close resources
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }// nothing we can do
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }//end finally try
        }//end try
//        System.out.println("Goodbye!");
    }

    //~~~~~~~ System Logging
    public void printFinalReport() {

        System.out.println("BEST TEST IS FROM TEST: " + mFastestTest + " WITH TIME OF: " + mLowestAverage);
        System.out.println(mWinningQuery);
    }

    public void printReport(double pAverageTime) {

        if (COMMAND_RUN_EXPLAIN_QUERY_TEST) { return; }
        mTestList.add(mCurrentOrder);
        NumberFormat formatter = new DecimalFormat("#0.00000");
        System.out.print("Test " + mtestCount + ": ");
        System.out.print(formatter.format(pAverageTime) + " secs");
        System.out.print(" -" + mCurrentOrder);
        System.out.println();
    }

    private String generateCombination(int[] pCombination) {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for (int i = 0; i < mIndeces.length; i++) {
            String index = mIndeces[pCombination[i]];
            sb.append(index);
            if (i == (mIndeces.length - 1)) {
                sb.append(" ASC);");
            } else {
                sb.append(" ASC, ");
            }
            mCurrentOrderList[i] = index;
        }

        String indexCombination = sb.toString();

        return indexCombination;
    }

    private void runIndexReassign(int[] combination) {
        Connection conn = null;
        Statement stmt = null;
        mtestCount++;

        if (COMMAND_RUN_QUERY_FAST && !COMMAND_RUN_OLD_QUERY) {
            if (mtestCount == 33 || mtestCount == 35 || mtestCount == 39 || mtestCount == 51 || mtestCount == 53 ||
                    mtestCount == 71 || mtestCount == 97 || mtestCount == 100 || mtestCount == 103) {  }
            else { return; }
        }

        String tableName = "`crossbird`.`" + mTableName + "`";
        String replaceIndex = "`i20131007_freezed_tag_id_status_id_company_id_publish_at`";

        if (COMMAND_RUN_OLD_QUERY) {
            replaceIndex = "`i20131007_freezed_status_id_company_id_publish_at`";
        }


        try {
            Class.forName("com.mysql.jdbc.Driver");

            conn = DriverManager.getConnection(DB_URL, USER, PASS);

            stmt = conn.createStatement();

            String indexes = generateCombination(combination);

            String sql = "ALTER TABLE " + tableName+ "\n" +
                    "DROP INDEX " + replaceIndex + ",\n" +
                    "ADD INDEX " + replaceIndex + " " + indexes;

            if (PRINT_DEBUG_SQLQUERY) { System.out.println(sql); };

            String currentCombo = "";
            for (int i = 0; i < combination.length; i++) {
                currentCombo = currentCombo + " " + (mIndeces[combination[i]]);
            }
            mTestList.add(currentCombo);
            mCurrentOrder = currentCombo;

            mCurrentQuery = sql.toString();
            stmt.executeUpdate(sql);
            runSelectQueryTest();

            stmt.close();
            conn.close();
        } catch (SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        } finally {
            //finally block used to close resources
            try {
                if (stmt != null)
                    stmt.close();
            } catch (SQLException se2) {
            }// nothing we can do
            try {
                if (conn != null)
                    conn.close();
            } catch (SQLException se) {
                se.printStackTrace();
            }//end finally try
        }//end try
//        System.out.println("Goodbye!");
    }

    private int[] generateIndices() {
        int[] indices = new int[mIndeces.length];
        for (int i = 0; i < mIndeces.length; i++) {
            indices[i] = i;
        }

        return indices;
    }

    private String generateRandomStatusID() {
        int status_id = 0;
        if (randomValue(100) < 50) {
            status_id = 1;
        } else {
            status_id = 3;
        }
        return Integer.toString(status_id);
    }

    private String generateRandomFreezed() {
        int freezed = 0;
        if (randomValue(100) < 5) {
            freezed = 2;
        }
        return Integer.toString(freezed);
    }

    private String generateRandomCompanyID() {
        int company_id = 110;
        if (randomValue(100) < 25) {
            company_id = 115;
        } else {
            company_id = 119;
        }
        return Integer.toString(company_id);
    }

    private String generateID() {
        NumberFormat nf = new DecimalFormat("000000000000");
        String id = (nf.format(mPopulateCount));
        return id;
    }

    private String generateTagID() {
        String[] tag_id = {"'abiste-abiste-1405320597413-tag-86ecee7b-e4ae-4e45-bd16-32045eeda626'",
                "'apparelweb-staffsnapteam-1410233030569-tag-de83c6e4-0841-4c03-a6d9-fff3cb8f23e6'",
                "'bals-francfranc-1403238069195-tag-9994cd8f-d83c-47bb-ad40-9b404e505c33'",
                "'onward-paulsmith-1424935437157-tag-dbadc618-10c0-4027-9195-5fb0fad985ff'",
                "'pal-threecoins-1435729378547-tag-ac74ae32-e2a6-4719-b5f7-44b9b13dac76'",
                "'pal-threecoins-1435729793876-tag-a6cc9784-d0f5-4176-b7d3-404ff50b75f9'",
                "'system-system-1402319141519-tag-e3051f04-d7c8-4765-85c0-1662a0af3e74'",
                "'system-system-1404735591059-tag-de479456-89bd-4efb-9fd9-2c2d75aae331'",
                "'system-system-1414665133304-tag-21b1041a-d55c-4830-a8f3-4b5f9361ab1f'",
                "'system-system-1421331225217-tag-058d7ab3-e87c-4473-b436-afbd48143def'",
                "'system-system-1433290282064-tag-c9c33624-71a3-4685-a199-44ec9c1ec3b3'",
                "'system-system-1434018212337-tag-b0fe14e2-d0d2-40e8-a325-da288d3ba64e'",
                "'system-system-1436969420905-tag-28ebb84e-f4bf-4c7f-a62e-eaffe13bbe4b'",
                "'system-system-1440064428407-tag-00cb0a2c-c509-4345-af49-e2d0176f8fd3'",
                "'system-system-1441272142422-tag-d1eeddfc-bed9-4ff8-8887-898e3d262186'",
                "'system-system-1441954221138-tag-4522bd8e-2bac-4d76-a460-87a9e5867a77'",
                "'system-system-1442487179175-tag-8e8612c4-94b2-4e8a-9b3d-0a1a96d58e7d'",
                "'system-system-1445567983204-tag-915d7384-c853-469a-943a-e4ceb219af71'",
                "'system-system-1457672438043-tag-d88f2173-e7c7-40c0-b11e-4e32f5747824'",
                "'system-system-1457673702495-tag-5ece6b3c-c1f2-4b30-b802-bc6ba5bde1e1'",
                "'system-system-1457955684034-tag-0c8bd2ba-cec1-4298-9210-65b1d6fc2772'"
        };

        String returnString;

        if (randomValue(100) < 30) {
            returnString = tag_id[randomValue(tag_id.length)];
        } else {
            returnString = "'system-system-1457955684034-tag-0c8bd2ba-cec1-4298-9210-65b1d6fc1234'";
        }

        return returnString;
    }

    public int randomValue(int pRange) {
        Random ran = new Random();
        int x = ran.nextInt(pRange);
        return x;
    }
}
