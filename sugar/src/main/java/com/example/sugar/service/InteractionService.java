package com.example.sugar.service;

import com.example.sugar.bean.InteractionPlayHour;
import com.example.sugar.bean.InteractionPlayTime;

import java.util.List;

public interface InteractionService {
    List<InteractionPlayHour> statsInteractionPlayHour(int date);

    List<InteractionPlayTime> statsInteractionPlayTime(int date);
}
