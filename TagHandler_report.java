package tradavo.admin;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import tradavo.api.model.Distributor;
import tradavo.app.AppException;
import tradavo.app.ApplicationGlobals;
import tradavo.app.DateUtil;
import tradavo.app.ErrorHelper;
import tradavo.app.LoginAuthenticate;
import tradavo.app.TagHandler;
import tradavo.app.UserValuesException;
import tradavo.aws.DistributorHelper;
import tradavo.production.PurchaseBalanceUpdate;
import tradavo.records.BrandHierarchyRecord;
import tradavo.records.InvoiceRecord;
import tradavo.records.PurchaseHistoryItemRecord;
import tradavo.records.PurchaseHistoryRecord;
import tradavo.reports.VendorMonthSummary;
import tradavo.utils.StringUtils;

public class TagHandler_report extends TagHandler {
	private Connection connection;
	private LoginAuthenticate auth;
	private String billedType = "";
	private PrintWriter pout;

	public void process() { 
		try {
			connection = getConnection("product");
			
			String txType = getAttributeValue( "txtype" );
			auth = (LoginAuthenticate)_userEnv.getValueObject("authenticator");
			if (txType.equals("daily"))
				txtypeGetDaily();
			else if (txType.equals("month_total")) 	// not in use
				txtypeMonthTotal();
			else if (txType.equals("weekly")) 	// not in use
				txtypeGetWeekly();
			else if (txType.equals("vendor_monthly"))
				txtypeVendorMonthly();
			else if (txType.equals("purchase_tabulation"))
				txtypePurchaseTabulation();
			else if (txType.equals("receivables_balance"))
				txtypeReceivablesBalance();
			else if (txType.equals("never_purchased"))
				txtypeNeverPurchased();
			else if  (txType.equals("salesforce_user_list"))
				txtypeSalesforceUserList();
			else if  (txType.equals("salesforce_acct_list"))
				txtypeSalesforceAccountList();
			else if  (txType.equals("frozen_daily"))
				txtypeFrozenDailySales();
			else if  (txType.equals("first_time_buyer"))
				txtypeFirstTimeBuyer();
			else if  (txType.equals("billing_purchases"))
				txtypeBillingPurchases();
			else if  (txType.equals("vendor_sum"))
				txtypeVendorSum();
			else if  (txType.equals("management_group_sales"))
				txtypeManagementGroupSales();
			else if  (txType.equals("account_sales"))
				txAccountSales();
			else if  (txType.equals("chart_balance"))
				txtypeChartBalance();
			else if  (txType.equals("vendor_month_sum"))
				txtypeVendorMonthSum();
			else if  (txType.equals("marriott_kickback"))
				txtypeMarriottKickback();
			else if  (txType.equals("account_recovery"))
				txtypeAccountRecovery();
			else if  (txType.equals("management_group_category_totals"))
				txtypeManagementGroupCategoryTotals();
			else if  (txType.equals("top_sellers"))
				txtypeTopSellers();
			else if  (txType.equals("dagger_report"))
				txtypeDaggerReport();
			else if  (txType.equals("update_sales_report"))
				txtypeUpdateDailySales();
		}
		catch (ClassNotFoundException e) {
 			System.out.println("Couldn't find the driver");
			errorHelper.addError(ErrorHelper.FATAL, "ClassNotFoundException " + e.getMessage());
		}
		catch (SQLException e) {
 			System.out.println("SQL problem: " + e.getMessage());
			printStackTrace( e );
			errorHelper.addError(ErrorHelper.FATAL, "SQLException " + e.getMessage());
		}
		catch (UserValuesException e) {
			printStackTrace( e );
			errorHelper.addError(ErrorHelper.FATAL, e.getMessage());
		}
		catch (IOException e) {
			printStackTrace( e );
			errorHelper.addError(ErrorHelper.FATAL, e.getMessage());
		}
		catch (AppException e) {
			errorHelper.addError(ErrorHelper.FATAL, e.getMessage());
		}
		finally {
			close(connection);
			int ret = errorHelper.outputMessages(_userEnv);
			_userEnv.putValue("_error_status", Integer.toString(ret));
		}
	}
	private void printVar(String var) {
		printVar(var, false);
	}
	private void printVar(String var, boolean lineEnd) {
		if (var == null)
			var = "";
		if (!lineEnd) {
			String s = csvString(var);
			pout.print(s + ",");
		}
		else
			pout.println(csvString(var));

	}
	private String csvString(String s) {
		StringBuilder sb;
		if (s.indexOf("\"") != -1) {
			sb = new StringBuilder();
			for (int q = 0; q < s.length(); q++) {
				sb.append(s.charAt(q));
				if (s.charAt(q) == '"')
					sb.append(s.charAt(q));
			}
		}
		else {
			sb = new StringBuilder(s);
		}
		if (sb.length() > 0 && sb.charAt(0) == '\'')           // wrap in quotes if there is no leading '
			return sb.toString();
		else
			return "\"" + sb.toString() + "\"";
	}
	private void printLastVar(String var) {
		printVar(var, true);
	}
	// Between start date and end date who didn't place an order for n days
	private void txtypeAccountRecovery() throws SQLException, UserValuesException {
		String rowns = this.getAttributeValue("rowns");
		String startdate = _userEnv.getValue("request.startdate");
		String enddate = _userEnv.getValue("request.enddate");
		String gap_days = _userEnv.getValue("request.gap_days");
		String buys = _userEnv.getValue("request.buys");
		int gap = 90, buy_sum = 6;
		if (gap_days != null) {
			try {
				gap = Integer.parseInt(gap_days);
			}
			catch (NumberFormatException e) {
			}
		}
		if (buys != null) {
			try {
				buy_sum = Integer.parseInt(buys);
			}
			catch (NumberFormatException e) {
			}
		}
		String query = 
			"SELECT order_id, p.account, left(purchase_time, 10) purchase_date, a.accountname, a.management_group  " +
			"from purchase_history p " +
			"INNER JOIN users.accounts a on a.account = p.account " +
			"where purchase_time between '2010-01-01' AND curdate() " +
			"group by order_id " +
			"order by p.account, purchase_time ";
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery(query);
		int startsum = 0, outputCount = 0;
		boolean inhibit = false;
		String lastAccount = "", lastDate = "";
		while (rs.next()) {
			String account = rs.getString("p.account");
			String accountname = rs.getString("a.accountname");
			String management_group = rs.getString("management_group");
			String purchase_date = rs.getString("purchase_date");
			if (account.equals(lastAccount)) {
				int purchase_gap = DateUtil.dateDiff(lastDate, purchase_date);
				if (purchase_gap >= gap) {
					if (startsum >= buy_sum) {
						_userEnv.putValue(rowns + ".account", account);
						_userEnv.putValue(rowns + ".accountname", accountname);
						_userEnv.putValue(rowns + ".management_group", management_group);
						_userEnv.putValue(rowns + ".startsum", startsum);
						_userEnv.putValue(rowns + ".purchase_gap", purchase_gap);
						_userEnv.putValue(rowns + ".purchase_date", purchase_date);
						startsum = 0;
						++outputCount;
					}
				}
			}
			else {
				startsum = 0;
				inhibit = false;
			}
			lastAccount = account;	
			lastDate = purchase_date;
			++startsum;
		}
		statement.close();
		_userEnv.putValue(rowns + "._rowcount", outputCount);
	}
	private void txtypeDaggerReport() throws SQLException {
		int count = 0;
		String rowns = getAttributeValue("rowns");
		String openQuery =
				"Create temporary table open_orders \n" +
				"SELECT p.order_id, p.userid, p.vendor, p.purchase_time, sum(i.quantity) quantity_sum, sum(i.ship_quantity) ship_sum\n" +
				" FROM purchase_history p\n" +
				" INNER JOIN users.accounts a ON p.account = a.account \n" +
				" INNER JOIN users.account_address aa ON aa.account = a.account \n" +
				" INNER JOIN purchase_history_item i ON p.order_id = i.order_id  AND p.vendor = i.distro \n" +
				" LEFT JOIN edi_purchase_head eph on p.order_id = eph.tradavo_orderid \n" +
				" LEFT JOIN users.account_group_detail poc on poc.account = a.account AND poc.groupid = 'poc_account' \n" +
				"WHERE date(purchase_time) >= DATE_SUB(now(),INTERVAL 60  day) \n" +
				"and i.itemid not like 'ship%'\n" +
				"\tGROUP BY p.order_id, p.vendor \n" +
				"having ship_sum > 0 and ship_sum != quantity_sum \n" +
				"\tORDER BY purchase_time desc, order_id, vendor";
		String itemQuery =
				"SELECT i.*, date(op.purchase_time) purchase_date, a.item_description, user_ident, user_desc from open_orders op\n" +
				"INNER join purchase_history_item i on i.order_id = op.order_id and i.distro = op.vendor\n" +
				" INNER JOIN amazon_product a on a.asin = i.itemid and a.distro = i.distro \n" +
				" INNER JOIN users.users u ON u.user_ident = op.userid \n" +
				" WHERE i.ship_quantity != quantity\n" +
				"and quantity != 0";
		Statement statement = connection.createStatement();
		statement.executeUpdate(openQuery);
		ResultSet rs = statement.executeQuery(itemQuery);
		while (rs.next()) {
			PurchaseHistoryItemRecord pir = new PurchaseHistoryItemRecord(rs, "i");
			String user_ident = rs.getString("user_ident");
			String user_desc = rs.getString("user_desc");
			pir.putEnv(_userEnv, rowns);
			String itemDescription = rs.getString("a.item_description");
			_userEnv.putValue(rowns + ".item_description", itemDescription);
			_userEnv.putValue(rowns + ".user_ident", user_ident);
			_userEnv.putValue(rowns + ".user_desc", user_desc);
			String purchase_date = rs.getString("purchase_date");
			_userEnv.putValue(rowns + ".purchase_date", purchase_date);
			String url = "https://merchant.tradavo.com/Tradavo/servlet/FormManager?tem=invoice_edit&txtype=view_order&orderid=" +
					pir.order_id;
			_userEnv.putValue(rowns + ".url", url);
			++count;
		}
		_userEnv.putValue(rowns + "._rowcount", count);
	}
	private String getBilledType(String orderid) throws SQLException {
		if (billedType.startsWith(orderid)) {
			String [] s = billedType.split("\\|");
			return s[1];
		}
		else {
			String billType = "b";
			Statement statement = connection.createStatement();
			ResultSet rs = statement.executeQuery(
				"SELECT factor_status FROM users.invoice " +
				" WHERE orderid = '" + orderid + "' AND factor_status in (1, 2)");
			if (rs.next())
				billType = "f";
			statement.close();
			billedType = orderid + "|" + billType;
			return billType;
		}
	}
	private void txtypeTopSellers() throws SQLException, UserValuesException, AppException {
		String rowns = getAttributeValue("rowns");
		String startDate = _userEnv.getValue("request.start_date");
		String merge = _userEnv.getValue("request.merge_master");
		if (merge == null)
			merge = "0";
		if (startDate == null || startDate.length() == 0)
			startDate = " date_sub(now(), interval 60 day)";
		else
			startDate = "'" + startDate + "'";
		String endDate = _userEnv.getValue("request.end_date");
		if (endDate == null || endDate.length() == 0)
			endDate = "now()";
		else
			endDate = "'" + endDate + "'";
		String limit = _userEnv.getValue("request.limit");
		String sort = _userEnv.getValue("request.sort");
		if (sort == null)
			sort = "purchase_count";
		else if (sort.equals("count"))
			sort = "purchase_count";
		else if (sort.equals("total"))
			sort = "total";
		else if (sort.equals("item"))
			sort = "item_description";
		if (limit == null) {
			limit = "100";
		}
		Statement statement = connection.createStatement();
		statement.executeUpdate("DROP TEMPORARY TABLE IF EXISTS sales_temp;");

		StringBuilder create = new StringBuilder(
				"CREATE TEMPORARY TABLE sales_temp\n" +
				"SELECT i.distro distro, i.itemid itemid, item_description, count(*) purchase_count, round(sum(price * quantity), 2) total, a.retail_cost, a.item_master_id\n" +
				"FROM  product.purchase_history p\n" +
				"INNER JOIN product.purchase_history_item i ON p.order_id = i.order_id AND p.vendor = i.distro\n" +
				"INNER JOIN product.amazon_product a on i.itemid = a.asin AND i.distro = a.distro\n" +
				"WHERE purchase_time >= " + startDate + " AND date(purchase_time) <= " + endDate + "\n");
				if (merge.equals("1"))
					create.append( " AND a.item_master_id = 0 \n")		;		// merge results, only get items not in the master here
				create.append( "GROUP by i.itemid, i.distro\n");
		statement.executeUpdate(create.toString());
		if (merge.equals("1")) {
			String insert =
					"INSERT INTO sales_temp\n" +
					"SELECT i.distro distro, i.itemid itemid, item_description, count(*) purchase_count, round(sum(price * quantity), 2) total, a.retail_cost, a.item_master_id\n" +
					"FROM  product.purchase_history p\n" +
					"INNER JOIN product.purchase_history_item i ON p.order_id = i.order_id AND p.vendor = i.distro\n" +
					"INNER JOIN product.amazon_product a on i.itemid = a.asin AND i.distro = a.distro\n" +
					"WHERE purchase_time >= " + startDate + " AND date(purchase_time) <= " + endDate + "\n" +
					" AND a.item_master_id != 0 \n" +
					"GROUP by a.item_master_id \n";
			statement.executeUpdate(insert);
		}
		String query =
				"SELECT t.*, g.category from sales_temp t\n" +
				" inner join product_group_item g on g.asin = t.itemid and g.distro = t.distro\n" +
				" WHERE g.active = 1 \n" +
				" ORDER BY " + sort + " DESC \n" +
				" limit " + limit;
		ResultSet rs = statement.executeQuery(query);

		String last_itemid = "", last_distro = "", last_item_description = "", last_purchase_count = "",
				last_total = "", last_retail_cost = "", last_item_master_id = "", last_category = "";
		ArrayList<String>categoryList = new ArrayList<>();
		String lastKey = "";
		int count = 0;
		while (rs.next()) {
			String itemid = rs.getString("itemid");
			String distro = rs.getString("distro");
			String item_description = csvString(rs.getString("item_description"));
			String purchase_count = rs.getString("purchase_count");
			String total = rs.getString("total");
			String retail_cost = rs.getString("retail_cost");
			String item_master_id = rs.getString("item_master_id");
			String category = rs.getString("category");
			if (lastKey.length() > 0 && !lastKey.equals(itemid + "|" + distro)) {
				_userEnv.putValue(rowns + ".itemid", last_itemid);
				_userEnv.putValue(rowns + ".distro", last_distro);
				_userEnv.putValue(rowns + ".item_description", last_item_description);
				_userEnv.putValue(rowns + ".purchase_count", last_purchase_count);
				_userEnv.putValue(rowns + ".total", last_total);
				_userEnv.putValue(rowns + ".retail_cost", last_retail_cost);
				_userEnv.putValue(rowns + ".item_master_id", last_item_master_id);
				for (int i = 1; i <= 2; i++) {		// output 2 categories.
					if (categoryList.size() < i)
						_userEnv.putValue(rowns + ".category" + i, "");
					else
						_userEnv.putValue(rowns + ".category" + i, categoryList.get(i - 1));
				}
				categoryList = new ArrayList<>();
				++count;
			}
			last_itemid = itemid;
			last_distro = distro;
			last_item_description = item_description;
			last_purchase_count = purchase_count;
			last_total = total;
			last_retail_cost = retail_cost;
			last_item_master_id = item_master_id;
			last_category = category;
			categoryList.add(category);
			lastKey = itemid + "|" + distro;
		}
		_userEnv.putValue(rowns + ".itemid", last_itemid);
		_userEnv.putValue(rowns + ".distro", last_distro);
		_userEnv.putValue(rowns + ".item_description", last_item_description);
		_userEnv.putValue(rowns + ".purchase_count", last_purchase_count);
		_userEnv.putValue(rowns + ".total", last_total);
		_userEnv.putValue(rowns + ".retail_cost", last_retail_cost);
		_userEnv.putValue(rowns + ".item_master_id", last_item_master_id);
		for (int i = 1; i <= 2; i++) {		// output 2 categories.
			if (categoryList.size() < i)
				_userEnv.putValue(rowns + ".category" + i, "");
			else
				_userEnv.putValue(rowns + ".category" + i, categoryList.get(i - 1));
		}
		++count;
		_userEnv.putValue(rowns + "._rowcount", count);
	}
	private void txtypeMarriottKickback() throws SQLException, UserValuesException, AppException {
		int count = 0;
		BigDecimal totalExtended = new BigDecimal(0);
		String rowns = this.getAttributeValue("rowns");
		String startdate = _userEnv.getValue("request.startdate");
		String enddate = _userEnv.getValue("request.enddate");
		if (startdate == null || startdate.length() == 0)
			startdate = DateUtil.getDate();
		if (enddate == null || enddate.length() == 0)
			enddate = DateUtil.getDate();
		int datediff = DateUtil.dateDiff(startdate,enddate);
		if (datediff > 92)
			throw new AppException("Size limit of report is three months (92 days)");
		String query = 
		"SELECT p.purchase_time, p.account, a.accountname, pi.order_id, pi.itemid, pi.distro, z.item_description, \n" +
		" pi.quantity, pi.price, round((pi.quantity * pi.price) , 2) extended, b.brand  \n" +
		"	FROM purchase_history p \n" +
		" INNER JOIN purchase_history_item pi ON pi.order_id = p.order_id and pi.distro = p.vendor \n" +
		" INNER JOIN users.accounts a ON a.account = p.account ANd a.management_group like 'marriott international%' \n" +
		" INNER JOIN amazon_product z ON z.asin = pi.itemid and z.distro = pi.distro \n" +
		" LEFT JOIN brand_hierarchy b ON b.itemid = z.asin" +
		" WHERE left(purchase_time, 10) >= '" + startdate + "' AND  left(purchase_time, 10) <= '" + enddate + "'\n" +
		"  AND p.vendor != 'msi' \n" +
		"  AND b.brand not like 'frito-lay%' \n" +
		" ORDER BY purchase_time, p.order_id, p.vendor";
		Statement statement = connection.createStatement();		
		ResultSet rs = statement.executeQuery(query);
		while (rs.next()) {
			String purchase_time = rs.getString("p.purchase_time");
			String account = rs.getString("p.account");
			String accountname = rs.getString("a.accountname");
			String orderid = rs.getString("pi.order_id");
			String itemid = rs.getString("pi.itemid");
			String distro = rs.getString("pi.distro");
			String item_description = rs.getString("z.item_description");
			String quantity = rs.getString("pi.quantity");
			String price = rs.getString("pi.price");
			String extended = rs.getString("extended");
			
			_userEnv.putValue(rowns + ".purchase_time", purchase_time);
			_userEnv.putValue(rowns + ".account", account);
			_userEnv.putValue(rowns + ".accountname", accountname);
			_userEnv.putValue(rowns + ".orderid", orderid);
			_userEnv.putValue(rowns + ".itemid", itemid);
			_userEnv.putValue(rowns + ".distro", distro);
			_userEnv.putValue(rowns + ".item_description", item_description);
			_userEnv.putValue(rowns + ".quantity", quantity);
			_userEnv.putValue(rowns + ".price", price);
			_userEnv.putValue(rowns + ".extended", extended);
			
			BigDecimal dex = new BigDecimal(extended);
			totalExtended = totalExtended.add(dex);
			++count;
		}
		_userEnv.putValue(rowns + "._rowcount", count);
		_userEnv.putValue(rowns + ".total", totalExtended.toString());
		statement.close();
	}
	private void txtypeVendorMonthSum() throws SQLException, UserValuesException {
		String rowns = this.getAttributeValue("rowns");
		String startdate = _userEnv.getValue("request.startdate");
		String enddate = _userEnv.getValue("request.enddate");
		if (startdate == null || startdate.length() == 0)
			startdate = DateUtil.getDate();
		if (enddate == null || enddate.length() == 0)
			enddate = DateUtil.getDate();
		VendorMonthSummary v = new VendorMonthSummary(connection, _userEnv, rowns);
		v.report(startdate, enddate);
	}
	private void txtypeChartBalance() throws SQLException, UserValuesException {
		int count = 0;
		String rowns = this.getAttributeValue("rowns");
		String query =
			"SELECT  mid(notes, 3, 3) chart, year(timestamp) year, month(timestamp) month, sum(amount) payments \n" +
			" FROM users.invoice_payment \n" +
			" WHERE notes like '![%' AND void != 1 \n" +
			" GROUP BY  mid(notes, 3, 3) , year(timestamp), month(timestamp)";
		Hashtable paymentTable = new Hashtable();
		Statement statement = connection.createStatement();		
		ResultSet rs = statement.executeQuery(query);
		while (rs.next()) {
			String chart = rs.getString("chart");
			String year = rs.getString("year");
			String month = rs.getString("month");
			BigDecimal payments = rs.getBigDecimal("payments");
			StringBuffer key = new StringBuffer(chart + year);
			if (month.length() == 1)
				key.append("0");
			key.append(month);
			paymentTable.put(key.toString(), payments);
		}
		query =  
			"SELECT c.*, year(timestamp) year, month(timestamp) month, sum(amount) charges \n" +
			" FROM users.invoice_charge  i \n" +
			"  INNER JOIN users.chart c ON c.chart = i.chart \n" +
			"  GROUP by chart, year(timestamp), month(timestamp) \n" +
			"  ORDER BY chart, year(timestamp), month(timestamp)";
		rs = statement.executeQuery(query);
		while (rs.next()) {
			String chart = rs.getString("c.chart");
			String chart_description = rs.getString("c.description");
			String year = rs.getString("year");
			String month = rs.getString("month");
			BigDecimal charges = rs.getBigDecimal("charges");
			StringBuffer key = new StringBuffer(chart + year);
			if (month.length() == 1)
				key.append("0");
			key.append(month);
			BigDecimal payment = (BigDecimal)paymentTable.get(key.toString());
			if (payment == null)
				payment = new BigDecimal(0).setScale(2);
			_userEnv.putValue(rowns + ".chart", chart);
			_userEnv.putValue(rowns + ".chart_description", chart_description);
			_userEnv.putValue(rowns + ".date", year + "-" + (month.length()==1?("0"+month):month));
			_userEnv.putValue(rowns + ".charges", charges.toString());
			_userEnv.putValue(rowns + ".payments", payment.toString());
			BigDecimal balance = charges.subtract(payment);
			_userEnv.putValue(rowns + ".balance", balance.toString());
			++count;
		}
		_userEnv.putValue(rowns + "._rowcount", count);
		statement.close();
  	}
	private void txtypeManagementGroupSales() throws SQLException, UserValuesException {
		String rowns = this.getAttributeValue("rowns");
		String factored = _userEnv.getValue("request.factored");
		String billed = _userEnv.getValue("request.billed");
		int start_month = 0, start_year = 0, count = 0;;
		String last_group = null;
		String [] monthTotals = { "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0" };
		Statement statement = connection.createStatement();
		
		ResultSet rs = statement.executeQuery(
			"select month(date_sub(curdate(), interval 5 month)) start_month, year(date_sub(curdate(), interval 5 month)) start_year");
		if (rs.next()) {
			start_month = rs.getInt("start_month");
			start_year = rs.getInt("start_year");
		}
		StringBuffer query = new StringBuffer(
			"SELECT g.groupid, g.group_description management_group, year(purchase_time) pyear, month(purchase_time) pmonth, date_format(purchase_time, '%M') pmonth_name, " +
			"		 round(sum(quantity * price), 2) total \n" +
			" FROM product.purchase_history p \n" +
			" INNER JOIN users.management_groups g ON p.management_group_id = g.groupid \n" +
			" INNER JOIN product.purchase_history_item pi ON p.order_id = pi.order_id AND p.vendor = pi.distro\n" +
			" WHERE purchase_time  > left(date_sub(curdate(), interval 5 month), 7) \n");
			if (billed != null && billed.equals("1"))
				query.append(" AND p.cardid = 'hltmjj5y'");
			query.append(
			" GROUP BY g.groupid, left(purchase_time, 7) \n" +
			" ORDER BY management_group, year(purchase_time) desc, month(purchase_time) desc \n");
		rs = statement.executeQuery(query.toString());
		while (rs.next()) {
			String management_group = rs.getString("management_group");
			if (last_group != null && management_group.equals(last_group) == false) {
				_userEnv.putValue(rowns + ".management_group", last_group);
				for (int i = 0; i < 6; i++) {
					int index = start_month + i;
					if (index > 12)
						index -= 12;
					_userEnv.putValue(rowns + ".month" + i + "_total", monthTotals[index]);
					monthTotals[index] = "0";
				}
				++count;
			}
			int pmonth = rs.getInt("pmonth");
			String total = rs.getString("total");
			monthTotals[pmonth] = total;
			last_group = management_group;
		}
		_userEnv.putValue(rowns + ".management_group", last_group);
		for (int i = 0; i < 6; i++) {
			int index = start_month + i;
			if (index > 12)
				index -= 12;
			_userEnv.putValue(rowns + ".month" + i + "_total", monthTotals[index]);
			monthTotals[index] = "0";
		}
		DecimalFormat format = new DecimalFormat("00");

		for (int i = 0; i < 6; i++, ++start_month) {
			if (start_month > 12) {
				++start_year;
				start_month = 1;
			}
			String head = start_year + "-" + format.format(start_month);
			_userEnv.putValue(rowns + ".month_head" + i, head);
		}
		_userEnv.putValue(rowns + "._rowcount", ++count);
 	}
	private void txAccountSales() throws SQLException, UserValuesException {
		String rowns = this.getAttributeValue("rowns");
		int start_month = 0, start_year = 0, count = 0;;
		String last_account = null, last_accountname = null;
		
		String [] monthTotals = { "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0" };
		Statement statement = connection.createStatement();
		
		ResultSet rs = statement.executeQuery(
			"select month(curdate()) start_month, year(date_sub(curdate(), interval 1 year)) start_year");
		if (rs.next()) {
			start_month = rs.getInt("start_month");
			start_year = rs.getInt("start_year");
		}
		StringBuffer query = new StringBuffer(
			"SELECT a.account, a.accountname, year(purchase_time) pyear, month(purchase_time) pmonth, date_format(purchase_time, '%M') pmonth_name, " +
			"		 round(sum(quantity * price), 2) total \n" +
			" FROM product.purchase_history p, users.accounts a, product.purchase_history_item pi \n");
			query.append(
			" WHERE purchase_time  > left(date_sub(curdate(), interval 12 month), 7) \n" +
			" AND p.account = a.account \n" +
			" AND p.order_id = pi.order_id \n" +
			" AND p.vendor = pi.distro \n" +
			" AND a.account_type = 'M' \n" +
			" AND a.active = 1 \n");
			query.append(
			" GROUP BY a.account, accountname, left(purchase_time, 7) \n" +
			" ORDER BY accountname, year(purchase_time) desc, month(purchase_time) desc \n");
		rs = statement.executeQuery(query.toString());
		while (rs.next()) {
			String account = rs.getString("account");
			String accountName = rs.getString("accountname");
			if (last_account != null && account.equals(last_account) == false) {
				writeAccountTotals(rowns, last_account, last_accountname, monthTotals, start_month);
				++count;
			}
			int pmonth = rs.getInt("pmonth");
			int pyear = rs.getInt("pyear");
			if(start_month == pmonth && (start_year + 1) == pyear) {
				pmonth = 0;
			}
			String total = rs.getString("total");
			monthTotals[pmonth] = total;
			last_account = account;
			last_accountname = accountName;
		}
		// Write account totals for last account
		writeAccountTotals(rowns, last_account, last_accountname, monthTotals, start_month);
		DecimalFormat format = new DecimalFormat("00");

		for (int i = 0; i < 13; i++, ++start_month) {
			if (start_month > 12) {
				++start_year;
				start_month = 1;
			}
			String head = start_year + "-" + format.format(start_month);
			_userEnv.putValue(rowns + ".month_head" + i, head);
		}
		_userEnv.putValue(rowns + "._rowcount", ++count);
 	}
	private void writeAccountTotals(String rowns, String last_account, String last_accountname, String[] monthTotals, int start_month) {
		// Write account with its totals.  Then reset all totals to zero
		_userEnv.putValue(rowns + ".account", last_account);
		_userEnv.putValue(rowns + ".accountname", last_accountname);
		for (int i = 0; i < 12; i++) {
			int index = start_month + i;
			if (index > 12)
				index -= 12;
			_userEnv.putValue(rowns + ".month" + i + "_total", monthTotals[index]);
			monthTotals[index] = "0";
		}
		_userEnv.putValue(rowns + ".month12_total", monthTotals[0]);
		monthTotals[0] = "0";
	}

	private void txtypeBillingPurchases() throws SQLException, UserValuesException {
		int count = 0;
		String lastDate = "";
		DecimalFormat format = new DecimalFormat("###,###.00");
		BigDecimal dayTotal = new BigDecimal(0);
		BigDecimal billTotal = new BigDecimal(0);
		BigDecimal factorTotal = new BigDecimal(0);
		String startDate = DateUtil.addDays(DateUtil.getDate(), - 90);
		String rowns = this.getAttributeValue("rowns");
		String query = 
			"SELECT p.*, round(sum(pi.quantity * pi.price), 2) total, i.* \n" +
			" FROM product.purchase_history p \n" +
			" INNER JOIN product.purchase_history_item pi ON p.order_id = pi.order_id \n" +
			" 	 AND p.vendor = pi.distro \n" +
			" INNER JOIN  users.bill_card c ON p.cardid = c.cardid \n" +
			" LEFT JOIN users.invoice i ON i.orderid = p.order_id \n" +			
			" WHERE purchase_time > '" + startDate + "' \n" +
			" GROUP BY p.order_id \n" +
			" ORDER BY left(purchase_time, 10) desc \n";
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery(query);
		while (rs.next()) {
			PurchaseHistoryRecord pr = new PurchaseHistoryRecord(rs, "p");

			InvoiceRecord ir = new InvoiceRecord(rs, "i");
			BigDecimal orderTotal = rs.getBigDecimal("total");
			String date = StringUtils.left(pr.purchase_time, 10);
			if (lastDate.length() > 0 && lastDate.equals(date) == false) {
				_userEnv.putValue(rowns + ".date", lastDate);
				_userEnv.putValue(rowns + ".day_total",  format.format(dayTotal));
				_userEnv.putValue(rowns + ".bill_total",  format.format(billTotal));
				_userEnv.putValue(rowns + ".factor_total",  format.format(factorTotal));
				dayTotal = new BigDecimal(0);
				billTotal = new BigDecimal(0);
				factorTotal = new BigDecimal(0);
				++count;
			}
			dayTotal = dayTotal.add(orderTotal);
			if (ir.invoice != null) {
				billTotal = billTotal.add(ir.order_total);
				if (ir.factor_status >= 1 && ir.factor_status <= 3)
					factorTotal = factorTotal.add(ir.order_total);
			}
			lastDate = date;
		}
		_userEnv.putValue(rowns + ".date", lastDate);
		_userEnv.putValue(rowns + ".day_total",  format.format(dayTotal));
		_userEnv.putValue(rowns + ".bill_total",  format.format(billTotal));
		_userEnv.putValue(rowns + ".factor_total",  format.format(factorTotal));
		statement.close();
		_userEnv.putValue(rowns + "._rowcount", ++count);
  	}
	private void txtypeFrozenDailySales() throws SQLException, UserValuesException {
		DecimalFormat format = new DecimalFormat("###,###.00");
		Hashtable totals = new Hashtable();
		String rowns = this.getAttributeValue("rowns");
		String query =
			" SELECT  WEEK(min(left(purchase_time, 10))) week, round(sum(price * quantity), 2) total  \n" +
			" FROM purchase_history p, purchase_history_item pi  \n" +
			" WHERE p.order_id = pi.order_id  \n" +
			" AND p.vendor = pi.distro  \n" +
			" AND purchase_time > '" + DateUtil.addDays(DateUtil.getDate(), -120) + "' \n" +
			" GROUP BY WEEK(LEFT(purchase_time, 10)) ";
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery(query);
		while (rs.next()) {
			String total = rs.getString("total");
			String week = rs.getString("week");
			totals.put(week, total);
		}	
		query =
			" SELECT  WEEK(LEFT(purchase_time, 10)) week,  min(LEFT(purchase_time, 10)) pdate,  \n" +
			"	i.category, ROUND(SUM(price * quantity), 2) category_total  \n" +
			" FROM purchase_history p, purchase_history_item pi, product_group_item i  \n" +
			" WHERE p.order_id = pi.order_id  \n" +
			" AND p.vendor = pi.distro  \n" +
			" AND i.asin = pi.itemid  \n" +
			" AND i.distro = pi.distro  \n" +
			" AND i.category in ('frozen dinners', 'ice cream')  \n" +
			" AND purchase_time > '" + DateUtil.addDays(DateUtil.getDate(), -120) + "' \n" +
			" GROUP BY YEAR(LEFT(purchase_time, 10)), WEEK(LEFT(purchase_time, 10)), category  \n" +
			" ORDER BY YEAR(LEFT(purchase_time, 10)) desc, WEEK(LEFT(purchase_time, 10)) desc, category ";
		statement = connection.createStatement();
		rs = statement.executeQuery(query);
		int count = 0;
		while (rs.next()) {
			String week = rs.getString("week");
			String pdate = rs.getString("pdate");
			String category = rs.getString("category");
			double category_total = rs.getDouble("category_total");
			double week_total = Double.parseDouble((String)totals.get(week));
			double pct = (category_total / week_total) * 100;
			_userEnv.putValue(rowns + ".week", week);
			_userEnv.putValue(rowns + ".pdate", pdate);
			_userEnv.putValue(rowns + ".category", category);
			_userEnv.putValue(rowns + ".week_total",  format.format(week_total));
			_userEnv.putValue(rowns + ".category_total",  format.format(category_total));
			_userEnv.putValue(rowns + ".pct", format.format(pct));
			++count;
		}
		_userEnv.putValue(rowns + "._rowcount", count);
	}
	private void txtypeGetWeekly() throws SQLException, UserValuesException {
		int count = 0;
		String rowns = this.getAttributeValue("rowns");
		String months = _userEnv.getValue("request.months");
		if (months == null)
			months = "25";
		String summary = _userEnv.getValue("summary");
		PurchaseBalanceUpdate.updateDaily(connection, 30);
		PreparedStatement summaryPs = connection.prepareStatement(
			" SELECT date, year(date) year, week(date) week, sum(total) total \n"  +
			"  FROM daily_sales \n"  +
			"  WHERE date > DATE_SUB(CURDATE(),INTERVAL ? month) \n"  +
			" GROUP BY year(date), week(date) \n"  +
			" ORDER BY year(date) desc, week(date) desc");
		summaryPs.setString(1, months);
		ResultSet rs = summaryPs.executeQuery();
		while (rs.next()) {
			String date = rs.getString("date");
			String year = rs.getString("year");
			String week = rs.getString("week");
			String total = rs.getString("total");
			_userEnv.putValue(rowns + ".date", date);
			_userEnv.putValue(rowns + ".year", year);
			_userEnv.putValue(rowns + ".week", week);
			_userEnv.putValue(rowns + ".total", total);
			++count;
		}
		_userEnv.putValue(rowns + "._rowcount", count);
		summaryPs.close();
	}

  	private void txtypeUpdateDailySales() throws SQLException {
		String update_days = getAttributeValue("update_days");
		int updateDays = 365;
		if (update_days == null)
			updateDays = 365;
		else
			updateDays = Integer.parseInt(update_days);
		PurchaseBalanceUpdate.updateDaily(connection, updateDays);
	}
  	private void txtypeGetDaily() throws SQLException, UserValuesException {
		int count = 0;
		PurchaseBalanceUpdate.updateDaily(connection, 14);
		ArrayList dailyList = new ArrayList();
		String startdate = "2007-01-01";
		String rowns = this.getAttributeValue("rowns");
		String months = getAttributeValue("months");
		DecimalFormat format = new DecimalFormat("###,###.00");
		BigDecimal weekTotal = new BigDecimal(0);
		BigDecimal monthTotal = new BigDecimal(0);
		if (months != null && months.length() > 0) {
			if (months.equals("0") == false) {
				int days = Integer.parseInt(months);
				startdate = DateUtil.addDays(DateUtil.getDate(), -(30*days));
				startdate = startdate.substring(0, 8).concat("01");	// first of month always
			}
		}
		// always start 6 d. prior to startdate to make sure a week total is complete
		String startQueryDate = DateUtil.addDays(startdate, -6);
		StringBuffer q = new StringBuffer(
			"SELECT date ptime, DAYOFWEEK(date) day_of_week, \n" + 
			"	DAYOFMONTH(date) day_of_month, ROUND(sum(total), 2) daily \n" +
			" FROM daily_sales " +
			" WHERE date > '" + startQueryDate + "' \n" +
			" GROUP BY date" +
			" ORDER BY date");
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery(q.toString());
		while (rs.next()) {
			String purchase_date = rs.getString("ptime");
			BigDecimal daily_total = rs.getBigDecimal("daily");
			int day_of_week = rs.getInt("day_of_week");			
			int day_of_month = rs.getInt("day_of_month");
			if (day_of_week == 1) 
				weekTotal = new BigDecimal(0);
			if (day_of_month == 1) 
				monthTotal = new BigDecimal(0);
			weekTotal = weekTotal.add(daily_total);
			monthTotal = monthTotal.add(daily_total);
			DailyTotalRecord dtr = new DailyTotalRecord(
				purchase_date, day_of_week, day_of_month, daily_total, weekTotal, monthTotal);
			dailyList.add(dtr);
		}
		// read it out backwards.
		for (int i = dailyList.size() - 1; i >= 0; i--) {
			DailyTotalRecord dtr = (DailyTotalRecord)dailyList.get(i);
			if (dtr.purchase_date.compareTo(startdate) < 0)
				break;
			String thirtyDayFormat = "";
			if (i >= 30) {
				BigDecimal thirtyDayTotal = new BigDecimal(0);
				for (int j = 0; j < 30; j++) {
					DailyTotalRecord jtr = (DailyTotalRecord)dailyList.get(i - j);
					thirtyDayTotal = thirtyDayTotal.add(jtr.dailyTotal);
				}
				thirtyDayFormat = format.format(thirtyDayTotal);
			}
			_userEnv.putValue(rowns + ".date", dtr.purchase_date);
			_userEnv.putValue(rowns + ".dayofweek", dtr.day_of_week);
			_userEnv.putValue(rowns + ".dayofmonth", dtr.day_of_month);
			_userEnv.putValue(rowns + ".thirty_day_total", thirtyDayFormat);
			String formatted = format.format(dtr.dailyTotal);
			_userEnv.putValue(rowns + ".daily_total", formatted);
			formatted = format.format(dtr.weekTotal);
			_userEnv.putValue(rowns + ".week_total", formatted);
			formatted = format.format(dtr.monthTotal);
			_userEnv.putValue(rowns + ".month_total", formatted);
			
			++count;
		}
		rs = statement.executeQuery( 
			"SELECT sum(total) ytd_total FROM daily_sales s " +
			 " WHERE year(s.date) = year(curdate())");
		if (rs.next()) {
			String formatted = format.format(rs.getDouble("ytd_total"));
			_userEnv.putValue(rowns + ".ytd_total", formatted);
		}
		statement.close();
		_userEnv.putValue(rowns + "._rowcount", count);
	}
	private void txtypeReceivablesBalance() throws SQLException {
		// this number is comprised of all receivables - all payments.
		// the pmi number of outstanding is also included.
 		DecimalFormat format = new DecimalFormat("######.00");		
 		DecimalFormat dformat = new DecimalFormat("###.0000");		
		String rowns = this.getAttributeValue("rowns");
		String factored = _userEnv.getValue("request.factored");
		if (factored == null)
			factored = "0";
		String enddate = getAttributeValue("enddate");
		if (enddate == null || enddate.length() == 0)
			enddate = DateUtil.getDate();
		ArrayList chargeKeys = new ArrayList();
		Hashtable chargeTable = new Hashtable();
		Hashtable payTable = new Hashtable();
		BigDecimal totalBilled = new BigDecimal(0);
		BigDecimal totalPaid = new BigDecimal(0);
		
		StringBuffer chargeQuery = new StringBuffer(
			"SELECT left(purchase_time, 7) pmonth, round(sum(pi.quantity * pi.price), 2) month_sum " +
			"  FROM purchase_history p \n" +
			"  INNER JOIN purchase_history_item pi \n" +
			"    ON pi.order_id = p.order_id \n" +
			"    AND pi.distro = p.vendor \n" +
			"  INNER JOIN users.bill_card b ON p.cardid = b.cardid\n");
			if (factored != null && factored.equals("1"))
				chargeQuery.append(
				"INNER JOIN users.invoice i ON i.orderid = p.order_id AND i.factor_status in (1, 2, 3) ");
			chargeQuery.append(
			"	WHERE left(p.purchase_time, 10) <= '" + enddate + "' \n" +
			"  GROUP BY left(purchase_time, 7) \n" +
			"  ORDER BY left(purchase_time, 7)");
		StringBuffer payQuery = new StringBuffer(
			"SELECT left(post_date, 7) pmonth, sum(amount) month_sum \n" +
			" FROM users.invoice_payment p\n");
			if (factored != null && factored.equals("1"))
				payQuery.append(
			" INNER JOIN users.invoice i ON i.orderid = p.orderid AND i.factor_status in (1, 2, 3) ");
			payQuery.append(
			" WHERE p.post_date <= '" + enddate + "' \n" +
			" AND p.void = 0 \n " +
			" GROUP BY left(p.post_date, 7)");
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery(chargeQuery.toString());
		while (rs.next()) {
			String pmonth = rs.getString("pmonth");
			BigDecimal month_sum = rs.getBigDecimal("month_sum");
			totalBilled = totalBilled.add(month_sum);
			chargeTable.put(pmonth, month_sum);
			chargeKeys.add(pmonth);
		}
		rs = statement.executeQuery(payQuery.toString());
		while (rs.next()) {
			String pmonth = rs.getString("pmonth");
			BigDecimal month_sum = rs.getBigDecimal("month_sum");
			totalPaid = totalPaid.add(month_sum);
			payTable.put(pmonth, month_sum);
		}
		BigDecimal monthBalance = new BigDecimal(0);
		BigDecimal cumulativeCharges =  new BigDecimal(0);
		BigDecimal cumulativePayments = new BigDecimal(0);
		for (int i = 0; i < chargeKeys.size(); i++) {
			String key = (String)chargeKeys.get(i);
			BigDecimal monthCharge = (BigDecimal)chargeTable.get(key);
			BigDecimal monthPayment = (BigDecimal)payTable.get(key);
			monthBalance = monthBalance.add(monthCharge);
			if (monthPayment == null)
				monthPayment = new BigDecimal(0);
			monthBalance = monthBalance.subtract(monthPayment);
			cumulativeCharges = cumulativeCharges.add(monthCharge);
			cumulativePayments = cumulativePayments.add(monthPayment);
			double paymentRatio = Double.parseDouble(cumulativePayments.toString()) / Double.parseDouble(cumulativeCharges.toString());
			
			_userEnv.putValue(rowns + ".month", key);
			_userEnv.putValue(rowns + ".charges", format.format(monthCharge));
			_userEnv.putValue(rowns + ".payments", format.format(monthPayment));
			_userEnv.putValue(rowns + ".cumulative_charges", format.format(cumulativeCharges));
			_userEnv.putValue(rowns + ".cumulative_payments", format.format(cumulativePayments));
			_userEnv.putValue(rowns + ".payment_ratio", dformat.format(paymentRatio));
			_userEnv.putValue(rowns + ".balance", format.format(monthBalance));
		}
		_userEnv.putValue(rowns + ".total_paid", format.format(totalPaid));
		_userEnv.putValue(rowns + ".total_billed", format.format(totalBilled));
		_userEnv.putValue(rowns + "._rowcount", chargeKeys.size());
	}
	private void txtypeNeverPurchased() throws SQLException, UserValuesException {
		String rowns = this.getAttributeValue("rowns");
		int registered_days = 0, count = 0;
		try {
			pout = new PrintWriter(new FileWriter(ApplicationGlobals.getInstance().getReportPath() + "never_purchased.csv"), true);
		}
		catch (IOException e) {
			throw new UserValuesException("IOException " + e.getMessage());
		}
		String [] columnHeadings = {
			"account","accountname","management_group","registered_days","user_description","title","user_email",
			"address","city","state","zipcode","phone","username","password","network","network_type"
		};
		for (int i = 0; i < columnHeadings.length; i++) {
			if (i < columnHeadings.length - 1)
				printVar(columnHeadings[i]);
			else
				printLastVar(columnHeadings[i]);
		}
		String query =
			"SELECT a.*, aa.*, u.*, n.networkname, n.network_type \n" +
			" FROM users.accounts a \n" +
			" INNER JOIN users.users u \n" +
			"    ON a.account = u.account \n" +
			" INNER JOIN users.account_address aa \n" +
			"    ON a.account = aa.account \n" +
			" LEFT JOIN users.network n on n.network = a.network \n" +
			" LEFT JOIN purchase_history p on p.account = a.account \n" +	// ok
			" WHERE p.order_id is null \n" +
			"    AND account_type = 'm' \n" +
			"    AND a.active = 1 \n" +
			" ORDER by create_date";
		Statement statement = connection.createStatement()	;
		ResultSet rs = statement.executeQuery(query);
		while (rs.next()) {
			AccountRecord ar = new AccountRecord(rs, "a");
			AccountAddressRecord aar = new AccountAddressRecord(rs, "aa");
			UserRecord ur = new UserRecord(rs, "u");
			if (ar.create_date != null && ar.create_date.length() > 0)
				registered_days = DateUtil.dateDiff(ar.create_date, DateUtil.getDate());
			else
				registered_days = -1;
			String networkname = rs.getString("n.networkname");
			String network_type = rs.getString("n.network_type");
			printVar(ar.account);
			printVar(ar.accountname);
			printVar(ar.management_group);
			printVar("" + registered_days);
			printVar(ur.user_desc);
			printVar(ur.user_title);
			printVar(ur.user_mail);
			printVar(aar.ship_street); 
			printVar(aar.ship_city); 
	   		printVar(aar.ship_state);
	   		printVar(aar.ship_zip);
	   		printVar(aar.ship_phone_format);
	   		printVar(ur.username);
	   		printVar(ur.userpass);
	   		printVar(networkname);
  			printLastVar(network_type);
			++count;
		}
		_userEnv.putValue(rowns + ".url", "../production/reports/never_purchased.csv");
		_userEnv.putValue(rowns + "._rowcount", count);
  		statement.close();
  		pout.close();
	}
	private void txtypeSalesforceUserList() throws SQLException, UserValuesException, IOException {
		String reportPath = ApplicationGlobals.getInstance().getReportPath();
		String filename = "salesforce_user_list.csv";
		pout = new PrintWriter(new FileWriter(reportPath + filename), true);

		int count = 0;
		boolean showInactiveUsers = false, showInactiveAccouts = false;
		String show_inactive_users = _userEnv.getValue("request.show_inactive_users");
		if (show_inactive_users != null && show_inactive_users.equals("yes"))
			showInactiveUsers = true;
		String show_inactive_accounts = _userEnv.getValue("request.show_inactive_accounts");
		if (show_inactive_accounts != null && show_inactive_accounts.equals("yes"))
			showInactiveUsers = true;
		String rowns = this.getAttributeValue("rowns");
		HashMap<String,String>purchaseMap = new HashMap<>();
		StringBuilder purchaseQuery = new StringBuilder(
				"SELECT u.user_ident, left(purchase_time, 10) purchase_date FROM users.users u\n" +
				" INNER JOIN users.accounts a on a.account = u.account\n" +
				" INNER JOIN product.purchase_history p ON p.userid = u.user_ident\n" +
				" WHERE account_type = 'm' \n");
		if (!showInactiveAccouts)
			purchaseQuery.append("  AND a.active = 1  \n" );
		if (!showInactiveUsers)
			purchaseQuery.append("  AND u.privilege < 99\n");
		purchaseQuery.append(
				" GROUP BY user_ident, order_id \n" +
				" ORDER BY u.user_ident, purchase_time desc");
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery(purchaseQuery.toString());
		int purchaseCount = 0;
		String lastUserid = "";
		while (rs.next()) {
			String userId = rs.getString("user_ident");
			String purchaseDate = rs.getString("purchase_date");
			if (!userId.equals(lastUserid)) {
				if (purchaseCount != 0) {
					String purchase = purchaseMap.get(lastUserid);
					purchaseMap.put(lastUserid, purchase + "|" + purchaseCount);
				}
				purchaseMap.put(userId, purchaseDate);
				purchaseCount = 0;
			}
			++purchaseCount;
			lastUserid = userId;
		}
		String purchase = purchaseMap.get(lastUserid);
		purchaseMap.put(lastUserid, purchase + "|" + purchaseCount);
		final String [] headings = {
				"tradavo account #",
				"accountname",
				"mamagement_group",
				"user id",
				"contact full name",
				"title",
				"user_email",
				"login",
				"referral_source",
				"last purchase",
				"purchase count",
				"days since registration",
				"user_active",
				"account_active"
		} ;
		for (int i = 0; i < headings.length; i++) {
			printVar(headings[i]);
		}
		printLastVar("");
		StringBuilder query = new StringBuilder(
			"SELECT a.account, a.accountname, a.management_group, u.user_ident, u.user_desc, u.user_title, u.password_created," +
			" u.user_mail,u.username,a.referral_source,if(u.privilege<99,'true','false') user_active, if(a.active=1,'true','false') account_active FROM users.accounts a \n" +
			" INNER JOIN users.users u ON u.account = a.account \n" +
			" INNER JOIN users.network n ON a.network = n.network \n" +
			" INNER JOIN users.account_address aa ON a.account = aa.account \n" +
			"	WHERE account_type = 'm' \n");
		if (!showInactiveAccouts)
			query.append(" AND a.active = 1 \n");
		if (!showInactiveUsers)
			query.append("  AND u.privilege < 99");
		rs = statement.executeQuery(query.toString());
		ArrayList<String>valueList = new ArrayList<>();
		while (rs.next()) {
			String account = rs.getString("a.account");
			printVar(account);
			String accountname = rs.getString("accountname");
			printVar(accountname);
			String management_group = rs.getString("management_group");
			printVar(management_group);
			String user_ident = rs.getString("user_ident");
			printVar(user_ident);
			String user_desc = rs.getString("user_desc");
			printVar(user_desc);
			String user_title = rs.getString("user_title");
			printVar(user_title);
			String user_mail = rs.getString("user_mail");
			printVar(user_mail);
			String username = rs.getString("username");
			printVar(username);
			String referral_source = rs.getString("referral_source");
			printVar(referral_source);

			purchase = purchaseMap.get(user_ident);
			if (purchase == null) {
				printVar("never");
				printVar("0");
			}
			else {
				String[] purchases = purchase.split("\\|");
				printVar(purchases[0]);
				printVar(purchases[1]);
			}
			String password_created = rs.getString("password_created");
			DateTime today = new DateTime();
			DateTime createDate = new DateTime(password_created);
			long days = Days.daysBetween(createDate, today).getDays();;
			printVar("" + days);
			String user_active = rs.getString("user_active");
			printVar(user_active);
			String account_active = rs.getString("account_active");
			printVar(account_active);
			printLastVar("");
			++count;
		}
		statement.close();
		pout.close();
		_userEnv.putValue(rowns + "._rowcount", count);		
		_userEnv.putValue(rowns + ".filename", "salesforce_user_list.csv");
	}
	private void txtypeSalesforceAccountList() throws SQLException, UserValuesException {
		int count = 0;
		String rowns = this.getAttributeValue("rowns");
  		Statement statement = connection.createStatement();
  		Hashtable twcTable = new Hashtable();
		createLastPurchaseTempTable(statement);
		try {
			pout = new PrintWriter(new FileWriter(ApplicationGlobals.getInstance().getReportPath() + "salesforce_account_list.csv"), true);
		}
		catch (IOException e) {
			throw new UserValuesException("IOException " + e.getMessage());
		}
		String [] heading = {
			"accountname","tradavo account #","management company","corporate_account_id","property_code","ship street",
			"ship street2","ship city","ship state","ship country","ship zip","ship phone","fax","contact full name","contact title",
			"ship email","date of registration","last order","orders placed","network_name","network_type",
			"billing","taxid","referral_source","discount_group","priority_fulfillment","account_active","planogram_flag","visuality_checkout"
		};
		printHeading(heading);
		
		// , if(length(c.cardid) > 1, 'billing', '') isbilled
		String query =
		"SELECT a.*, aa.*, n.*, t.*, x.taxid, b.account \n" +
		"	FROM users.accounts a \n" +
		"  INNER JOIN users.account_address aa ON a.account = aa.account \n" +
		"  LEFT JOIN users.network n ON a.network = n.network \n" + // ok
		"	LEFT JOIN temp_tabulation t on t.account = a.account \n" +	// ok
		"	LEFT JOIN users.bill_account b on b.account = a.account \n" +  // ok
		"  LEFT JOIN users.taxid x ON a.account = x.account \n" +
		"	WHERE account_type = 'm' \n" +
		"  AND a.active != 647";		// 647 is just a bogus number
		ResultSet rs = statement.executeQuery(query);
		while (rs.next()) {
			AccountRecord ar = new AccountRecord(rs, "a");
			printVar(ar.accountname);
			printVar(ar.account);
			printVar(ar.management_group);
			printVar(ar.corporate_account_id);
			printVar(ar.property_code);
			
			AccountAddressRecord aar = new AccountAddressRecord(rs, "aa");
			printVar(aar.ship_street);
			printVar(aar.ship_street2);
			printVar(aar.ship_city);
			printVar(aar.ship_state);
			printVar(aar.ship_zip);
			printVar(aar.ship_country);
			printVar(aar.ship_phone);
			printVar(aar.fax);
			printVar(aar.ship_contact);
			printVar(aar.contact_title);
			printVar(aar.ship_email);
			printVar(ar.create_date);
			String last_purchase_date = rs.getString("t.last_purchase");
			// if (last_purchase_date == null || last_purchase_date.length() == 0)
			//	System.out.println("last purchase date null");
			String purchase_count = rs.getString("t.purchase_count");
			// if (purchase_count == null || purchase_count.length() == 0)
			//	System.out.println("purchase_count null");
			String purchase_days = "";
			if (last_purchase_date != null) {
				int pdays = DateUtil.dateDiff(last_purchase_date, DateUtil.getDate());
				purchase_days = "" + pdays;
			}
			else {
				purchase_days = "";
			}
			printVar(purchase_days);
			printVar(purchase_count);
			NetworkRecord nr = new NetworkRecord(rs, "n");
			printVar(nr.networkname);
			printVar(nr.network_type);
			String billed = rs.getString("b.account");
			if (billed != null)
				_userEnv.putValue(rowns + ".billed", "1");
			else
				_userEnv.putValue(rowns + ".billed", "0");
			printVar(billed);
			
			String taxid = rs.getString("x.taxid");
			if (taxid == null)
				taxid = "";
			printVar(taxid);
			printVar(ar.referral_source);
			printVar(ar.discount_group);

			printVar(ar.priority_fulfillment==1?"TRUE":"FALSE");
			printVar(ar.active==1?"TRUE":"FALSE");
			if (ar.visuality_flag == 1 || ar.visuality_flag == 2)
				printVar("TRUE");
			else
				printVar("FALSE");
			if (ar.visuality_flag == 2)
				printLastVar("TRUE");
			else
				printLastVar("FALSE");
			++count;
		}
		statement.close();
		pout.close();
		_userEnv.putValue(rowns + ".filename", "/production/reports/salesforce_account_list.csv");
		_userEnv.putValue(rowns + "._rowcount", count);		
 	}
	private void touchme() {
		// force rebuild.
	}
 	private void printHeading(String [] heading) {
 		for (int i = 0; i < heading.length; i++) {
 			if (i == (heading.length - 1))
  				printLastVar(heading[i]);
			else	
 				printVar(heading[i]);
 		}
 	}
	private void txtypePurchaseTabulation() throws SQLException, UserValuesException {
		int count = 0;
		String rowns = this.getAttributeValue("rowns");
		String view = _userEnv.getValue("request.view");
		StringBuffer userQuery = new StringBuffer(
		"SELECT a.accountname, u.*, t.* \n" +
		" FROM temp_tabulation t, users.accounts a, users.users u \n" +
		" WHERE t.account = a.account \n" +
		" AND a.account = u.account \n");
		if (view == null || view.length() == 0)
			userQuery.append(" GROUP BY a.account \n");
  		Statement statement = connection.createStatement();
		createLastPurchaseTempTable(statement);
		userQuery.append(" ORDER BY t.last_purchase desc, a.account");
		ResultSet rs = statement.executeQuery(userQuery.toString());
		while (rs.next()) {
			UserRecord ur = new UserRecord(rs, "u");
			String accountname = rs.getString("a.accountname");
			int purchase_count = rs.getInt("t.purchase_count");
			String last_purchase = rs.getString("t.last_purchase");
			int purchase_days = DateUtil.dateDiff(last_purchase, DateUtil.getDate());
			_userEnv.putValue(rowns + ".accountname", accountname);
			_userEnv.putValue(rowns + ".purchase_count", purchase_count);
			_userEnv.putValue(rowns + ".purchase_days", purchase_days);
			ur.putEnv(_userEnv, rowns);
			++count;
		}
		_userEnv.putValue(rowns + "._rowcount", count);
  		statement.executeUpdate("drop table temp_tabulation;");
  		statement.close();
	}
	private void createLastPurchaseTempTable(Statement statement) throws SQLException {
		String createStatement =
		"CREATE TEMPORARY TABLE temp_tabulation ( account char(12), last_purchase char(10),  \n" +
		" purchase_count int(4),  primary key (account) );";
		String purchaseQuery =
		" INSERT INTO temp_tabulation \n" +
		" SELECT p.account account, max(left(purchase_time, 10)) last_purchase, \n" +
		"   count(distinct order_id) purchase_count \n" +
		" FROM product.purchase_history p \n" +
		" WHERE purchase_target != 0 \n" +
		" GROUP BY p.account \n";
		statement.executeUpdate(createStatement);
		statement.executeUpdate(purchaseQuery);
	}

	private void txtypeMonthTotal() throws SQLException {
		int count = 0;
		String rowns = this.getAttributeValue("rowns");
		String month = "", year = "";
		String indate = getAttributeValue("date");
		String query =
			"SELECT MONTH('" + indate + "') m, YEAR('" + indate + "') y";
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery(query);
		if (rs.next()) {
			month = rs.getString("m");
			year = rs.getString("y");
		}
		query = 
			"SELECT MONTH(purchase_time) month, FORMAT(SUM(price * quantity), 2) total \n" +
			" FROM purchase_history p, purchase_history_item pi \n" +
			" WHERE p.order_id = pi.order_id and p.vendor = pi.distro \n" +
			" AND MONTH(purchase_time) = " + month + " AND YEAR(purchase_time) = " + year +
			" GROUP BY YEAR(purchase_time), MONTH(purchase_time)  \n" +
			" ORDER BY YEAR(purchase_time), MONTH(purchase_time) ";
		statement = connection.createStatement();
		rs = statement.executeQuery(query);
		while (rs.next()) {
			String total = rs.getString("total");
			_userEnv.putValue(rowns + ".total", total);
			++count;
		}
		_userEnv.putValue(rowns + "._rowcount", count);
	}
	private void txtypeFirstTimeBuyer() throws SQLException, UserValuesException, IOException {
		// first, get the maximum date
		String rowns = this.getAttributeValue("rowns");
	
		String maxtime = "2020-12-31";
		String query =	
			"SELECT left(max(p.purchase_time), 10) mtime " +
			"  FROM users.first_time_buyer f \n" +
			" INNER JOIN purchase_history p on p.order_id = f.orderid";
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery(query);
		if (rs.next()) {
			maxtime = rs.getString("mtime");
			if (maxtime == null)
				maxtime = "2008-11-01 23:59:59";
		}
		else 
			maxtime = "2008-11-01 23:59:59";
		String delete = 
			"DELETE FROM users.first_time_buyer " +
			" WHERE left(purchase_time, 10) = left('" + maxtime + "', 10)";
		
		// statement.executeUpdate(delete);
		String nextday = DateUtil.addDays(StringUtils.left(maxtime, 10), 1);
		String lastday = DateUtil.addDays(StringUtils.left(maxtime, 10), -1);
		// update thru today
		PreparedStatement ps = connection.prepareStatement(
		"REPLACE into users.first_time_buyer (orderid, account, amount) \n " +
		" SELECT p.order_id,  p.account, '0' \n " +
		"  FROM purchase_history p \n " +
		"  LEFT JOIN purchase_history q \n" +
		"    ON p.account = q.account and left(q.purchase_time, 10) < ? \n " +
		"  WHERE p.purchase_time > ? \n" +
		"  AND left(p.purchase_time, 10) < ? \n" +
		"  AND q.account is null \n " +
		"  GROUP by p.account");
		String today = DateUtil.getDate();
		while (DateUtil.compare(StringUtils.left(maxtime, 10), today) <= 0) {
			ps.setString(1, lastday);
			ps.setString(2, maxtime);
			ps.setString(3, nextday);
			
			ps.execute();
			lastday = StringUtils.left(maxtime, 10);
			maxtime = nextday;
			nextday = DateUtil.addDays(maxtime, 1);
		}
		ps.close();
		query = 
			"REPLACE into users.first_time_buyer (orderid, account, amount) \n " +
			"SELECT f.orderid, f.account, sum(i.price * i.quantity) amount " +
			"  FROM users.first_time_buyer f \n" +
			" INNER JOIN purchase_history_item i on i.order_id = f.orderid \n" +
			"  WHERE f.amount = '0' " +
			"  GROUP by f.orderid ";
		statement.executeUpdate(query);
		// spit out the list...
		query = 
			"SELECT left(p.purchase_time,10) purchase_date, f.amount, a.account, a.accountname, a.management_group, aa.ship_phone, n.networkname, n.network_type \n" +
			" FROM users.first_time_buyer f \n" +
			" INNER JOIN purchase_history p ON p.order_id = f.orderid \n" +
			" INNER JOIN users.accounts a ON a.account = f.account \n" +
			" INNER JOIN users.account_address aa on aa.account = a.account \n" +
			" INNER JOIN users.network n on a.network = n.network \n" +
			" GROUP BY f.orderid \n" +
			" ORDER BY p.purchase_time desc";
		rs = statement.executeQuery(query);
		int count = 0;
		pout = new PrintWriter(new FileWriter(ApplicationGlobals.getInstance().getReportPath() + "first-time-buyer.csv"), true);
		pout.println("purchase_time,account,accountname,management_group,amount,phone");
		while (rs.next()) {
			String purchase_time = rs.getString("purchase_date");
			String account = rs.getString("a.account");
			String amount = rs.getString("f.amount");
			String accountname = rs.getString("accountname");
			String management_group = rs.getString("management_group");
			String phone = rs.getString("ship_phone");
			pout.print(purchase_time + ",");
			pout.print(account + ",");
			pout.print("\"" + accountname + "\",");
			pout.print("\"" + management_group + "\",");
			pout.print(amount + ",");
			pout.println(phone);
			/*
			_userEnv.putValue(rowns + ".purchase_time", purchase_time);
			_userEnv.putValue(rowns + ".account", account);
			_userEnv.putValue(rowns + ".accountname", accountname);
			_userEnv.putValue(rowns + ".management_group", management_group);
			*/
			++count;
		}
		pout.close();
		_userEnv.putValue(rowns + ".filename", "../production/reports/first-time-buyer.csv");
		_userEnv.putValue(rowns + "._rowcount", count);
		statement.close();
 	}
 	private void txtypeVendorSum() throws SQLException, UserValuesException {
 		String [] vendors = { "a", "l", "msi", "eb", "tf", "tl", "tb", "tc", "td", "pmi" };
 		String lastMonth = "";
 		int count = 0;
 		boolean found = false;
		String rowns = this.getAttributeValue("rowns");
		DecimalFormat format = new DecimalFormat("#,###,###");
 		String query =
		"select  left(purchase_time, 7) pmonth, p.vendor, round(sum(pi.price * pi.quantity), 2) total " +
		"	from purchase_history p, purchase_history_item pi " +
		"	where purchase_time > '2008-01' " +
		"	and p.order_id = pi.order_id " +
		"	and p.vendor = pi.distro " +
		"	group by left(purchase_time, 7), p.vendor " +
		"	order by left(purchase_time, 7) desc, vendor ";
		Hashtable vendorTotals = new Hashtable();
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery(query);
		while (rs.next()) {
			String pmonth = rs.getString("pmonth");
			String vendor = rs.getString("vendor");
			BigDecimal total = rs.getBigDecimal("total");
			if (lastMonth.length() > 0 && pmonth.equals(lastMonth) == false) {
				for (int i = 0; i < vendors.length; i++) {
					_userEnv.putValue(rowns + ".pmonth", lastMonth);
					BigDecimal vtotal = (BigDecimal)vendorTotals.get(vendors[i]);
					if (vtotal != null)
						_userEnv.putValue(rowns + ".total", format.format(vtotal));
					else
						_userEnv.putValue(rowns + ".total", "");
					++count;
				}
				vendorTotals = new Hashtable();
			}
			for (int i = 0; i < vendors.length; i++) {
				if (vendors[i].equals(vendor)) {
					vendorTotals.put(vendor, total);
					found = true;
					break;
				}
			}
			if (found == false) {
				BigDecimal pmiTotal = (BigDecimal)vendorTotals.get("pmi");
				if (pmiTotal == null)
					pmiTotal = new BigDecimal(0);
				pmiTotal = pmiTotal.add(total);
				vendorTotals.put("pmi", pmiTotal);
			}
			found = false;	
			lastMonth = pmonth;
		}
		for (int i = 0; i < vendors.length; i++) {
			_userEnv.putValue(rowns + ".pmonth", lastMonth);
			BigDecimal vtotal = (BigDecimal)vendorTotals.get(vendors[i]);
			if (vtotal != null)
				_userEnv.putValue(rowns + ".total", format.format(vtotal));
			else
				_userEnv.putValue(rowns + ".total", "");
			++count;
		}
		_userEnv.putValue(rowns + "._rowcount", count);
	}
	private void txtypeManagementGroupCategoryTotals() throws SQLException, UserValuesException {
		String rowns = getAttributeValue("rowns");
		String managementGroup = _userEnv.getValue("request.management_group");
		String startDate = _userEnv.getValue("request.start_date");
		String endDate = _userEnv.getValue("request.end_date");
		String query =
			" select a.accountname, g.category, round(sum(price * quantity), 2) total from users.accounts a" +
			" inner join purchase_history p on p.account = a.account" +
			" inner join purchase_history_item i on i.order_id = p.order_id and i.distro = p.vendor " +
			" inner join product_group_item g on g.asin = i.itemid and g.distro = i.distro" +
			" where management_group = '" + managementGroup + "'" +
			" and purchase_time > '" + startDate + "' AND left(purchase_time, 10) <= '" + endDate + "'" +
			" group by a.account, g.category";
		try {
			pout = new PrintWriter(new FileWriter(ApplicationGlobals.getInstance().getReportPath() + "management_group_category_totals.csv"), true);
		}
		catch (IOException e) {
			throw new UserValuesException("Couldn't open output file " + ApplicationGlobals.getInstance().getReportPath() + "management_group_category_totals.csv");
		}
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery(query);
		while (rs.next()) {
			String accountName = rs.getString("a.accountname");
			String category = rs.getString("g.category");
			String total = rs.getString("total");
			pout.print("\"" + accountName + "\",");
			pout.print("\"" + category + "\",");
			pout.println(total);
		}
		statement.close();
		pout.close();
		_userEnv.putValue(rowns + ".link", "/Tradavo/production/reports/management_group_category_totals.csv");
	}
	private void txtypeVendorMonthly() throws SQLException, UserValuesException {
		int count = 0;
		String purchase_date = null;
		BigDecimal month_total = new BigDecimal(0);
		BigDecimal month_confirmed = new BigDecimal(0);
		DecimalFormat format = new DecimalFormat("######.00");
	
		String rowns = this.getAttributeValue("rowns");
		String month = this.getAttributeValue("month");
		String distro = this.getAttributeValue("distro");
		
		String startDate = DateUtil.addDays(DateUtil.getDate(), -60);
		String request_date = _userEnv.getValue("request.startdate");
		if (request_date != null && request_date.length() > 0)
			startDate = request_date;
		Hashtable shipTable = new Hashtable();
		String shipQuery =
		"SELECT p.*, sum(s.quantity * pi.price) total " +
		" FROM purchase_history p, purchase_history_item pi, ship_history s \n" +
		" WHERE p.order_id = pi.order_id AND p.vendor = pi.distro \n" +
		" AND s.order_id = pi.order_id AND s.distro = pi.distro AND s.itemid = pi.itemid \n" +
		" AND p.purchase_target != 0 \n" +
 		" AND vendor = '" + distro + "'" +
 		" AND purchase_time > '" + startDate.substring(0, 7) + "'" +
 		" GROUP BY pi.order_id, pi.distro " +
 		" ORDER BY left(purchase_time, 10), p.order_id";
		Statement statement = connection.createStatement();
		ResultSet rs = statement.executeQuery(shipQuery);
		while (rs.next()) {
			PurchaseHistoryRecord pr = new PurchaseHistoryRecord(rs, "p");
			String total = rs.getString("total");
			shipTable.put(pr.order_id, total);
		}
		String query =
		"SELECT p.*, pi.*, a.accountname, sum(pi.quantity * pi.price) total \n" +
		" FROM purchase_history p, purchase_history_item pi, users.accounts a \n" +
		" LEFT JOIN purchase_final f on f.order_id = p.order_id and f.distro = p.vendor \n" +
		" LEFT JOIN purchase_credit c on c.order_id = p.order_id and c.distro = p.vendor \n" +
		" WHERE a.account = p.account \n" +
		" AND p.order_id = pi.order_id AND p.vendor = pi.distro \n" +
		" AND p.purchase_target != 0 \n" +
 		" AND vendor = '" + distro + "'" +
 		" AND purchase_time > '" + startDate.substring(0, 7) + "'" +
 		" GROUP BY pi.order_id, pi.distro " +
 		" ORDER BY left(purchase_time, 10), p.order_id";
		statement = connection.createStatement();
		rs = statement.executeQuery(query);
		ArrayList purchaseList = new ArrayList();
		while (rs.next()) {
			PurchaseHistoryRecord pr = new PurchaseHistoryRecord(rs, "p");
			String accountname = rs.getString("a.accountname");
			_userEnv.putValue(rowns + ".accountname", accountname);
			String invoice_total = rs.getString("total");
			String ship_total = (String)shipTable.get(pr.order_id);
			if (ship_total == null)
				ship_total = "0";
			pr.putEnv(_userEnv, rowns);
			_userEnv.putValue(rowns + ".orderid", pr.order_id);
			purchase_date = pr.purchase_time.substring(0, 10);
			_userEnv.putValue(rowns + ".purchase_date", purchase_date);
			String formatted = format.format(Double.parseDouble(invoice_total));
			_userEnv.putValue(rowns + ".invoice_total", formatted);
			formatted = format.format(Double.parseDouble(ship_total));
			_userEnv.putValue(rowns + ".confirmed_total", formatted);
			++count;
		}
		_userEnv.putValue(rowns + "._rowcount", count);
	}
	private void printVar(String var, int count) {
		StringBuilder sb = new StringBuilder(var);
		for (int i = 0; i < sb.length(); i++) {
			if (sb.charAt(i) == '"') {
				sb.insert(i, "\"");
				i++;
			}
		}
		if (count == 1)
			pout.println("\"" + sb + "\"");
		else
			pout.print("\"" + sb + "\",");
	}

	private class DailyTotalRecord {
		String purchase_date;
		BigDecimal dailyTotal, weekTotal, monthTotal;
		int day_of_week, day_of_month;
		
		DailyTotalRecord(String purchase_date, int day_of_week, int day_of_month,
					 BigDecimal dailyTotal, BigDecimal weekTotal, BigDecimal monthTotal ) {
			this.purchase_date = purchase_date;
			this.dailyTotal = dailyTotal;
			this.weekTotal = weekTotal;
			this.monthTotal = monthTotal;
			this.day_of_month = day_of_month;
			this.day_of_week = day_of_week;
		}
	}
}
