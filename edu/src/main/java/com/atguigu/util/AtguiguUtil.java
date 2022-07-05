package com.atguigu.util;

import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName AtguiguUtil
 * @Author Chris
 * @Description TODO
 * @Date 2022/7/4 9:00
 **/

public class AtguiguUtil {
	public static void main(String[] args) {

	}

	public static <T> List<T> toList(Iterable<T> elements) {
		List<T> list = new ArrayList<>();
		for (T element : elements) {
			list.add(element);
		}
		return list;
	}

	public static boolean compareLTZ(String one, String two) {
		String oneNoZ = one.replace("Z", "");
		String twoNoZ = two.replace("Z", "");
		return oneNoZ.compareTo(twoNoZ) >= 0;
	}
}
