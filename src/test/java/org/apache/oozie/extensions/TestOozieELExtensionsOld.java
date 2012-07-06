package org.apache.oozie.extensions;

import java.io.File;

import org.apache.oozie.client.OozieClient;
import org.apache.oozie.coord.CoordELFunctions;
import org.apache.oozie.coord.SyncCoordAction;
import org.apache.oozie.coord.SyncCoordDataset;
import org.apache.oozie.coord.TimeUnit;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.ELService;
import org.apache.oozie.service.Services;
import org.apache.oozie.util.DateUtils;
import org.apache.oozie.util.ELEvaluator;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestOozieELExtensionsOld {

    private ELEvaluator instEval;
    private ELEvaluator createEval;

    @BeforeClass
    public void setUp() throws Exception {
        String curPath = new File(".").getAbsolutePath();
        System.setProperty(Services.OOZIE_HOME_DIR, curPath);
        String confPath = new File(getClass().getResource("/oozie-site-old.xml").getFile()).getParent();
        System.setProperty(ConfigurationService.OOZIE_CONFIG_DIR, confPath);
        System.setProperty(ConfigurationService.OOZIE_CONFIG_FILE, "oozie-site-old.xml");
        Services.setOozieHome();

        Services services = new Services();
        services.getConf().set("oozie.services", "org.apache.oozie.service.ELService");
        services.init();

        instEval = Services.get().get(ELService.class).createEvaluator("coord-action-create-inst");
        instEval.setVariable(OozieClient.USER_NAME, "test_user");
        instEval.setVariable(OozieClient.GROUP_NAME, "test_group");
        createEval = Services.get().get(ELService.class).createEvaluator("coord-action-create");
        createEval.setVariable(OozieClient.USER_NAME, "test_user");
        createEval.setVariable(OozieClient.GROUP_NAME, "test_group");
    }

    @Test
    public void testDataIn() throws Exception {
        ELEvaluator eval = Services.get().get(ELService.class).createEvaluator("coord-action-start"); 
        String uris = "hdfs://localhost:8020/clicks/2010/01/01/00,hdfs://localhost:8020/clicks/2010/01/01/01";
        eval.setVariable(".datain.clicks", uris );
        String expuris = "hdfs://localhost:8020/clicks/2010/01/01/00/*/US,hdfs://localhost:8020/clicks/2010/01/01/01/*/US";
        Assert.assertEquals(expuris, CoordELFunctions.evalAndWrap(eval, "${elext:dataIn('clicks', '*/US')}"));
    }
    
    @Test
    public void testCurrentMonth() throws Exception {
        initForCurrentThread();

        String expr = "${elext:currentMonth(0,0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-01T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${elext:currentMonth(2,-1,0)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-02T23:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${elext:currentMonthWithOffset(2,-1,0,0)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-02T23:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testCurrentMonthWithOffset() throws Exception {
        initForCurrentThread();

        String expr = "${elext:currentMonth(2,-1,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-02T23:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        initForCurrentThread("2007-09-30T010:00Z", "2009-10-02T16:30Z", "2009-10-02T16:30Z");

        expr = "${elext:currentMonthWithOffset(2,-1,0,-43200)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-02T23:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        initForCurrentThread("2007-09-30T010:00Z", "2009-09-03T16:30Z", "2009-09-03T16:30Z");

        expr = "${elext:currentMonthWithOffset(2,-1,0,-1440)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-02T23:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        initForCurrentThread("2007-09-30T010:00Z", "2009-09-30T23:00Z", "2009-09-30T22:00Z");

        expr = "${elext:currentMonth(0,0,0)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-01T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        initForCurrentThread("2007-09-30T010:00Z", "2009-10-02T23:00Z", "2009-10-02T22:00Z");

        expr = "${elext:currentMonthWithOffset(0,0,0, -2880)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-01T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    private String getELExpression(String expr) {
        if(expr != null) {
            return "${" + expr + "}";
        }
        return null;
    }

    @Test
    public void testToday() throws Exception {
        initForCurrentThread();

        String expr = "${elext:today(0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-02T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${elext:today(1,-20)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-02T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${elext:todayWithOffset(1,-20, 0)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-02T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testTodayOffset() throws Exception {
        initForCurrentThread();

        String expr = "${elext:today(2,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-02T02:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        initForCurrentThread("2007-09-30T010:00Z", "2009-09-03T11:30Z", "2009-09-03T10:30Z");

        expr = "${elext:todayWithOffset(2,0,-1440)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-02T02:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        initForCurrentThread("2007-09-30T010:00Z", "2009-09-02T16:30Z", "2009-09-02T16:30Z");

        expr = "${elext:todayWithOffset(2,0,-240)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-02T02:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        initForCurrentThread("2007-09-30T010:00Z", "2009-09-03T16:30Z", "2009-09-03T15:30Z");

        expr = "${elext:todayWithOffset(2,0,-240)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertFalse("2009-09-02T02:00Z".equals(CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult))));
    }

    @Test
    public void testNow() throws Exception {
        initForCurrentThread();

        String expr = "${elext:now(0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-02T10:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${elext:now(2,-10)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-02T12:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testYesterday() throws Exception {
        initForCurrentThread();

        String expr = "${elext:yesterday(0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-01T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${elext:yesterday(1,10)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-01T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${elext:yesterdayWithOffset(1,10, 0)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-01T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testYesterdayWithOffset() throws Exception {
        initForCurrentThread();

        String expr = "${elext:yesterday(1,10)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-01T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        initForCurrentThread("2007-09-30T010:00Z", "2009-09-03T16:30Z", "2009-09-03T16:30Z");

        expr = "${elext:yesterdayWithOffset(1,10, -1440)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-09-01T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testLastMonth() throws Exception {
        initForCurrentThread();

        String expr = "${elext:lastMonth(0,0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-08-01T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${elext:lastMonth(1,1,10)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-08-02T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${elext:lastMonthWithOffset(1,1,10,0)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-08-02T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testLastMonthWithOffset() throws Exception {
        initForCurrentThread();

        String expr = "${elext:lastMonth(0,0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-08-01T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        initForCurrentThread("2007-09-30T010:00Z", "2009-09-03T16:30Z", "2009-09-03T16:30Z");

        expr = "${elext:lastMonthWithOffset(0,0,0,-1440)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-08-01T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testCurrentYear() throws Exception {
        initForCurrentThread();

        String expr = "${elext:currentYear(0,0,0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-01-01T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${elext:currentYear(1,0,1,0)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-02-01T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${elext:currentYearWithOffset(1,0,1,0,0)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-02-01T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testCurrentYearWithOffset() throws Exception {
        initForCurrentThread("2007-09-30T010:00Z", "2009-12-30T00:30Z", "2009-12-30T00:00Z");

        String expr = "${elext:currentYear(1,0,1,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-02-01T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        initForCurrentThread("2007-09-30T010:00Z", "2010-01-01T00:30Z", "2010-01-01T00:00Z");

        expr = "${elext:currentYearWithOffset(1,0,1,0,-2880)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2009-02-01T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testLastYear() throws Exception {
        initForCurrentThread();

        String expr = "${elext:lastYear(0,0,0,0)}";
        String instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2008-01-01T00:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${elext:lastYear(1,0,1,0)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2008-02-01T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));

        expr = "${elext:lastYearWithOffset(1,0,1,0, 0)}";
        instResult = CoordELFunctions.evalAndWrap(instEval, expr);
        Assert.assertEquals("2008-02-01T01:00Z", CoordELFunctions.evalAndWrap(createEval, getELExpression(instResult)));
    }

    @Test
    public void testNominalGreaterThanInitial() throws Exception {
        initForCurrentThread("2009-08-30T010:00Z", "2009-09-02T11:30Z", "2009-09-02T10:30Z");
        String expr = "${elext:currentYear(0,0,0,0)}";
        Assert.assertEquals("", CoordELFunctions.evalAndWrap(instEval, expr));
    }

    private void initForCurrentThread() throws Exception {
        initForCurrentThread("2007-09-30T010:00Z", "2009-09-02T11:30Z", "2009-09-02T10:30Z");
    }

    private void initForCurrentThread(String initialInstance, String actualTime, String nominalTime) throws Exception {
        SyncCoordAction appInst;
        SyncCoordDataset ds;
        ds = new SyncCoordDataset();
        ds.setFrequency(1);
        ds.setInitInstance(DateUtils.parseDateUTC(initialInstance));
        ds.setTimeUnit(TimeUnit.HOUR);
        ds.setTimeZone(DateUtils.getTimeZone("UTC"));
        ds.setName("test");
        ds.setUriTemplate("hdfs://localhost:9000/user/test_user/US/${YEAR}/${MONTH}/${DAY}");
        ds.setType("SYNC");
        ds.setDoneFlag("");

        appInst = new SyncCoordAction();
        appInst.setActualTime(DateUtils.parseDateUTC(actualTime));
        appInst.setNominalTime(DateUtils.parseDateUTC(nominalTime));
        appInst.setTimeZone(DateUtils.getTimeZone("UTC"));
        appInst.setActionId("00000-oozie-C@1");
        appInst.setName("mycoordinator-app");

        CoordELFunctions.configureEvaluator(instEval, ds, appInst);
        CoordELFunctions.configureEvaluator(createEval, ds, appInst);
    }
}
