package study.querydsl;

import com.querydsl.jpa.impl.JPAQueryFactory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Hello;
import study.querydsl.entity.QHello;
import static org.junit.jupiter.api.Assertions.*;

import javax.persistence.EntityManager;

@SpringBootTest
@Transactional
@Commit
class QuerydslApplicationTests {

	@Autowired
	private EntityManager em;

	@Test
	void contextLoads() {
		Hello hello = new Hello();
		em.persist(hello);

		JPAQueryFactory query = new JPAQueryFactory(em);
		QHello qHello = QHello.hello;

		Hello one = query
				.selectFrom(qHello)
				.fetchOne();

		assertEquals(hello, one);
		assertEquals(one.getId(), hello.getId());
	}

}
