package study.querydsl;

import com.querydsl.core.Tuple;
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
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceUnit;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.assertj.core.api.Assertions.*;
import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.team;

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

        Member member3 = new Member("member3", 10, teamB);
        Member member4 = new Member("member4", 10, teamB);

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

    @Test
    public void resultFetch() throws Exception {
        // given
        List<Member> memberList = query
                .selectFrom(member)
                .fetch();

        Member fetchOne = query
                .selectFrom(QMember.member)
                .fetchOne();

        Member fetchFirst = query
                .selectFrom(QMember.member)
                .fetchFirst();

        /**
         * MyISAM : 전체 row가 테이블에 저장되므로 Count(*) 의 시간복잡도 O(1)
         * InnoDB : full scan 필요 : O(n)
         */
        int size = query
                .selectFrom(member)
                .fetch().size();

    }

    /**
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순
     * 2. 회원 이름 올림차순
     * 2에서 회원 이름이 없으면 마지막에 출력 (nulls last)
     */
    @Test
    public void sortingTest() throws Exception {
        // given
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        // when
        List<Member> result = query
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.username.desc()
                        , member.age.asc().nullsLast())
                .fetch();

        Member member1 = result.get(0);
        Member member2 = result.get(1);
        Member member3 = result.get(2);

        // then
        assertAll(
                () -> assertEquals(member1.getUsername(), "member5"),
                () -> assertEquals(member2.getUsername(), "member6"),
                () -> assertNull(member3.getUsername())
        );
    }

    @Test
    public void paging() throws Exception {
        // given
        List<Member> result = query
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetch();
        // when
        for (Member member1 : result) {
            System.out.println(member1);
        }
        // then
        assertEquals(result.size(), 2);

    }

    @Test
    public void aggregation() throws Exception {
        // given
        Tuple tuple = query
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min()
                )
                .from(member)
                .fetchOne();
        // when
        assertAll(
                () -> assertEquals(tuple.get(member.count()), 4),
                () -> assertEquals(tuple.get(member.age.sum()), 40),
                () -> assertEquals(tuple.get(member.age.avg()), 10),
                () -> assertEquals(tuple.get(member.age.max()), 10),
                () -> assertEquals(tuple.get(member.age.min()), 10)
        );
        // then
    }

    @Test
    public void groupingTest() throws Exception {
        // given
        List<Tuple> result = query
                .select(
                        team.name,
                        member.age.avg()
                )
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();
        // when
        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        // then
        assertAll(
                () -> assertEquals(teamA.get(team.name), "teamA"),
                () -> assertEquals(teamA.get(member.age.avg()), 10),

                () -> assertEquals(teamB.get(team.name), "teamB"),
                () -> assertEquals(teamB.get(member.age.avg()), 10)
        );
    }

    @Test
    public void basicJoin() throws Exception {
        // given
        List<Member> members = query
                .selectFrom(member)
                .join(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();
        // when
        for (Member member1 : members) {
            System.out.println("member : " + member1);
        }
        // then
        assertThat(members)
                .extracting("username")
                .containsExactly("member1", "member2");
    }

    /**
     * 세타 조인
     * 회원의 이름이 팀 이름과 같은 회원 조회
     * DB마다 성능최적화를 하지만 방식이 다르다
     * member, team 테이블을 모두 join한 후에 이름 대
     */
    @Test
    public void theta_join() throws Exception {
        // given
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        // when
        List<Member> result = query
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();
        // then
        for (Member member1 : result) {
            System.out.println(member1);
        }
    }

    /**
     * 회원과 팀(팀 이름이 teamA인 팀)을 join, 회원은 모두 조회
     * JPQL : select m, t from Member m left join m.team t on t.name = 'teamA'
     */
    @Test
    public void join_on_filtering() throws Exception {
        // given
        List<Tuple> teamA = query
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team)
                .on(team.name.eq("teamA"))
                .fetch();
        // when
        for (Tuple tuple : teamA) {
            System.out.println("tuple : " + tuple);
        }
        // then
    }


    @Test
    public void join_on_no_relation() throws Exception {
        // given
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        // when
        List<Tuple> fetch = query
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team)
                .on(team.name.eq(member.username))
                .fetch();
        // then
        for (Tuple tuple : fetch) {
            System.out.println("tuple = " + tuple);
        }
    }

    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    public void noFetchJoin() throws Exception {
        // given
        em.flush();
        em.clear();

        Member findMember = query
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();
        // when
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        // then
        assertThat(loaded).as("join fetch 미적용").isFalse();
    }
    
    @Test
    public void fetchJoinOn() throws Exception {
        // given
        em.flush();
        em.clear();

        Member findMember = query
                .selectFrom(QMember.member)
                .join(QMember.member.team, team).fetchJoin()
                .where(QMember.member.username.eq("member1"))
                .fetchOne();
        // when
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        // then
        assertThat(loaded).as("join fetch 적용").isTrue();
    }


}
