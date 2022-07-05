package com.atguigu.app.dwd.db;

import com.atguigu.app.BaseSQLApp;
import com.atguigu.common.Constant;
import com.atguigu.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @ClassName DwdUserRegister
 * @Author Chris
 * @Description TODO
 * @Date 2022/7/5 14:03
 **/

public class DwdUserRegister extends BaseSQLApp {
	public static void main(String[] args) {
		new DwdUserRegister().init(
				2002,
				2,
				"DwdUserRegister",
				10
		);
	}

	@Override
	protected void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
		readOdsDb(tEnv, "DwdUserRegister");

		Table userInfo = tEnv.sqlQuery("select " +
				" data['id'] user_id," +
				" date_format(data['create_time'], 'yyyy-MM-dd' ) date_id," +
				" data['create_time'] create_time," +
				" cast(ts as string) ts " +
				" from ods_db" +
				" where `database`='gmall'" +
				" and `table`='user_info'" +
				" and `type`='insert'");

		tEnv.executeSql("create table `dwd_user_register`(" +
				"`user_id` string,  " +
				"`date_id` string,  " +
				"`create_time` string,  " +
				"`ts` string  " +
				")" + SQLUtil.getKafkaSinkDDL(Constant.TOPIC_DWD_USER_REGISTER));


		userInfo.executeInsert("dwd_user_register");
	}
}
