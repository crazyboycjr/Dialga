APP_SRCS := $(addprefix app/, server.cpp client.cpp)
APPS := $(patsubst app/%.cpp, build/app/%, $(APP_SRCS))

LDFLAGS += -Lbuild -Wl,-rpath=build -ldialga -lpthread

build/app/%: app/%.cpp dialga
	@mkdir -p $(@D)
	$(CXX) $(CFLAGS) $(INCPATH) -MM -MT build/app/$* $< >build/app/$*.d
	$(CXX) $(CFLAGS) $(INCPATH) -o $@ $(filter %.cpp %.a, $^) $(LDFLAGS)

-include build/app/*.d

clean_app:
	$(RM) $(APPS)
	$(RM) $(APPS:=.d)

.PHONY: clean_app
