package study.querydsl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import study.querydsl.dto.MemberDto;
import study.querydsl.dto.QMemberDto;
import study.querydsl.dto.UserDto;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;

import java.util.List;
import java.util.function.Supplier;

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

        Member member5 = new Member("member5", 30, teamB);
        Member member6 = new Member("member6", 40, teamB);

        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
        em.persist(member5);
        em.persist(member6);

//        em.flush();
//        em.clear();
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
        System.out.println(size);
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
                () -> assertEquals(member1.getUsername(), "member6"),
                () -> assertEquals(member2.getUsername(), "member5"),
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

    /**
     * 나이가 평균 이상인 회원 조회
     */
    @Test
    public void subQuery() throws Exception {
        // given
        QMember memberSub = new QMember("memberSub");

        em.persist(new Member("member6", 20));

        List<Member> members = query
                .selectFrom(member)
                .where(member.age.goe(
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(members).extracting("age")
                .containsExactly(20);
    }

    @Test
    public void caseTest() throws Exception {
        // given
        List<String> fetch = query
                .select(member.age
                        .when(10).then("열살")
                        .when(40).then("마흔")
                        .otherwise("기타"))
                .from(member)
                .fetch();
        // when
        for (String s : fetch) {
            System.out.println(s);
        }
        // then
    }

    @Test
    public void complexCase() throws Exception {
        // given
        List<String> result = query
                .select(new CaseBuilder()
                        .when(member.age.between(0, 20)).then("0~20살")
                        .when(member.age.between(21, 30)).then("21살~30살")
                        .otherwise("기타")
                )
                .from(member)
                .fetch();

        // when
        for (String s : result) {
            System.out.println(s);
        }
        // then
    }

    @Test
    public void constant() throws Exception {
        // given
        List<Tuple> result = query
                .select(member.username, Expressions.constant("A"))
                .from(member)
                .fetch();
        // when
        for (Tuple tuple : result) {
            System.out.println(tuple);
        }
        // then
    }

    @Test
    public void concat() throws Exception {
        // given
        List<String> fetch = query
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .where(member.age.eq(10))
                .fetch();
        // when
        for (String s : fetch) {
            System.out.println(s);
        }
        // then
    }
    
    @Test
    public void simpleProjection() throws Exception {
        // given
        List<String> result = query
                .select(member.username)
                .from(member)
                .fetch();
    }

    @Test
    public void tupleProjection() throws Exception {
        // given
        List<Tuple> result = query
                .select(member.username, member.age)
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println(tuple.get(member.username));
            System.out.println(tuple.get(member.age));
        }
        // when

        // then
    }

    @Test
    public void findDtoBySetter() throws Exception {
        // given
        List<Tuple> result = query
                .select(Projections.bean(MemberDto.class),
                        member.username,
                        member.age
                )
                .from(member)
                .fetch();
    }

    @Test
    public void findDtoByField() throws Exception {
        // given
        List<MemberDto> result = query
                .select(Projections.fields(MemberDto.class,
                        member.username,
                        member.age
                ))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println(memberDto);
        }
    }

    @Test
    public void findDtoByConstructor() throws Exception {
        // given
        List<MemberDto> result = query
                .select(Projections.constructor(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();
        for (MemberDto memberDto : result) {
            System.out.println(memberDto);
        }
    }

    @Test
    public void subQueryInProjection() throws Exception {
        QMember memberSub = new QMember("memberSub");

        List<UserDto> result = query
                .select(Projections.fields(UserDto.class,
                        member.username.as("name"),
                        ExpressionUtils.as(
                                JPAExpressions
                                        .select(memberSub.age.max())
                                        .from(memberSub),
                                "age"
                        )))
                .from(member)
                .fetch();
        for (UserDto tuple : result) {
            System.out.println(tuple);
        }
    }

    @Test
    public void findDtoByQueryProjection() throws Exception {
        List<MemberDto> result = query
                .select(new QMemberDto(member.username, member.age))
                .from(member)
                .fetch();
        for (MemberDto memberDto : result) {
            System.out.println(memberDto);
        }
    }

    @Test
    public void dynamicQuery_BooleanBuilder() throws Exception {
        // given
//        String usernameParam = "member1";
        String usernameParam = null;
        Integer ageParam = 10;

        List<Member> result = searchMember1(usernameParam, ageParam);
        for (Member member1 : result) {
            System.out.println(member1);
        }
//        assertEquals(result.size(), 1);
    }

    @Test
    public void dynamicQuery_WhereParam() throws Exception {
        String usernameParam = "member1";
        Integer ageParam = 10;

        List<Member> result = searchMember2(usernameParam, ageParam);
        assertEquals(result.size(), 1);

    }
    
    @Test
    public void nullHandling() throws Exception {
        // given
        List<Member> members = searchMember2(null, 10);
        // when
        for (Member member1 : members) {
            System.out.println(member1);
        }
        // then
    }

    @Test
    public void bulkUpdate() throws Exception {

        long count = query
                .update(member)
                 .set(member.username, "비회원")
                .where(member.age.lt(20))
                .execute();

        em.flush();
        em.clear();

        List<Member> fetch = query
                .selectFrom(member)
                .from(member)
                .fetch();

        for (Member fetch1 : fetch) {
            System.out.println(fetch1);
        }

    }

    @Test
    public void bulkAdd() throws Exception {
        long execute = query
                .update(member)
                .set(member.age, member.age.subtract(1))
                .execute();
    }

    @Test
    public void bulkDelete() throws Exception {
        long count = query
                .delete(member)
                .where(member.age.gt(18))
                .execute();
    }

    @Test
    public void sqlFunction() throws Exception {
        List<String> result = query
                .select(Expressions.stringTemplate(
                        "function('replace', {0}, {1}, {2})",
                        member.username, "member", "M"
                ))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println(s);
        }
    }
    
    @Test
    public void sqlFunction2() throws Exception {
        List<String> result = query
                .select(member.username)
                .from(member)
//                .where(member.username.eq(
//                        Expressions.stringTemplate("function('lower', {0})", member.username)
//                ))
                .where(member.username.eq(member.username.lower()))
                .fetch();

        for (String s : result) {
            System.out.println(s);
        }
    }

    private List<Member> searchMember1(String usernameCond, Integer ageCond) {

        BooleanBuilder builder = new BooleanBuilder();

        if (usernameCond != null) {
            builder.and(member.username.eq(usernameCond));
        }

        if (ageCond != null) {
            builder.and(member.age.eq(ageCond));
        }

        return query
                .selectFrom(member)
                .where(builder)
                .fetch();

    }

    private List<Member> searchMember2(String usernameCond, Integer ageCond) {
        return query
                .selectFrom(member)
                .where(allEq(usernameCond, ageCond))
                .fetch();
    }

    private Predicate allEq(String usernameCond, Integer ageCond) {
        return usernameEq(usernameCond).and(ageEq(ageCond));
    }

    private BooleanBuilder usernameEq(String usernameCond) {
        return nullSafeBuilder(() -> member.username.eq(usernameCond));

//        if (!StringUtils.hasText(usernameCond)) {
//            return new BooleanBuilder();
//        } else {
//            return new BooleanBuilder(member.username.eq(usernameCond));
//        }
    }

    private BooleanBuilder ageEq(Integer ageCond) {
        return nullSafeBuilder(() -> member.age.eq(ageCond));

//        if (ageCond == null) {
//            return new BooleanBuilder();
//        } else {
//            return new BooleanBuilder(member.age.eq(ageCond));
//        }
    }

    public static BooleanBuilder nullSafeBuilder(Supplier<BooleanExpression> f){
        try {
            return new BooleanBuilder(f.get());
        } catch (IllegalArgumentException e) {
            return new BooleanBuilder();
        }
    }

}
