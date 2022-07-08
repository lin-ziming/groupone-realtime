package com.example.sugar.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @ClassName PageViewType
 * @Author Chris
 * @Description TODO
 * @Date 2022/7/8 13:39
 **/
@Data
@AllArgsConstructor
public class PageViewType {
	//网页id
	String pageId;
	//浏览数量
	Long uvCt;

}
