raw_databrics_data = [
    {
        'result': [
            dict(
                ID=1,
                PARENT_ID=2,
                DEPENDS_ON=3,
                GROUP_ID=1,
                TITLE="test",
                DESCRIPTION="test desrc",
                TAGS="tag1",
                STATUS="todo",
                PRIORITY="hight",
                DEADLINE="2024-02-01",
            ),
        ],
        'next': 2
    },
    {
        'result': [
            dict(
                ID=2,
                PARENT_ID=2,
                DEPENDS_ON=3,
                GROUP_ID=1,
                TITLE="test",
                DESCRIPTION="test desrc",
                TAGS="tag1",
                STATUS="todo",
                PRIORITY="hight",
                DEADLINE="2024-02-01",
            ),
        ],
    },
]

ready_data = [
    dict(
        ID=1,
        PARENT_ID=2,
        DEPENDS_ON=3,
        GROUP_ID=1,
        TITLE="test",
        DESCRIPTION="test desrc",
        TAGS="tag1",
        STATUS="todo",
        PRIORITY="hight",
        DEADLINE="2024-02-01",
    ),
    dict(
        ID=2,
        PARENT_ID=2,
        DEPENDS_ON=3,
        GROUP_ID=1,
        TITLE="test",
        DESCRIPTION="test desrc",
        TAGS="tag1",
        STATUS="todo",
        PRIORITY="hight",
        DEADLINE="2024-02-01",
    ),
]
