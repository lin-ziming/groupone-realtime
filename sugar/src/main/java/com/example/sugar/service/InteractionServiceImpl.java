package com.example.sugar.service;

import com.example.sugar.bean.InteractionPlayHour;
import com.example.sugar.bean.InteractionPlayTime;
import com.example.sugar.mapper.InteractionMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.util.List;

@Service
public class InteractionServiceImpl implements InteractionService{

    @Autowired
    InteractionMapper interactionMapper;

    @Override
    public List<InteractionPlayHour> statsInteractionPlayHour(int date) {
        return interactionMapper.statsInteractionPlayHour(date);
    }

    @Override
    public List<InteractionPlayTime> statsInteractionPlayTime(int date) {
        return interactionMapper.statsInteractionPlayTime(date);
    }


}
