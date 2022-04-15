package study.querydsl;

import com.querydsl.jpa.impl.JPAQueryFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static study.querydsl.entity.QMember.member;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @Autowired
    EntityManager em;
    JPAQueryFactory query;

    @BeforeEach
    public void before(){
        // given
        query = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 10, teamA);

        Member member3 = new Member("member3", 10, teamA);
        Member member4 = new Member("member4", 10, teamA);

        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);

        em.flush();
        em.clear();
    }

    @Test
    public void jpqlTest() throws Exception {
        // given
        String queryString = "select m from Member m " +
                             "where m.username = :username";
        Member findMember = em.createQuery(queryString, Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertAll(
                () -> assertEquals("member1", findMember.getUsername())
        );
    }

    @Test
    public void querydslTest() throws Exception {
        // given

        Member findMember = query
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        assertAll(
                () -> assertEquals("member1", findMember.getUsername())
        );
    }

    @Test
    public void search() throws Exception {
        // given
        Member member1 = query
                .selectFrom(member)
                .where(member.username.eq("member1"),
                       member.age.eq(10)
                )
                .fetchOne();

        assertAll(
                () -> assertEquals(member1.getUsername(), "member1")
        );
        // when

        // then
    }
}
