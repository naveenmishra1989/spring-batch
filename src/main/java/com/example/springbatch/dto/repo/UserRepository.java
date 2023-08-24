package com.example.springbatch.dto.repo;

import com.example.springbatch.dto.User;
import org.springframework.data.jpa.repository.JpaRepository;

public interface UserRepository extends JpaRepository<User,Integer> {
}
