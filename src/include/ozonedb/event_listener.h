#ifndef OZONEDB_EVENT_LISTENER_H
#define OZONEDB_EVENT_LISTENER_H
namespace ozonedb {
class EventListener {
 public:
  virtual void onLogCompactionStart(){};
  virtual void onLogCompactionCompletion(int time){};
  virtual void onSSTableCompactionStart(){};
  virtual void onSSTableCompactionCompletion(int time, int level){};
  virtual void onViewUpdate(){};
  virtual void onNewTail(){};
};

}  // namespace ozonedb
#endif  // OZONEDB_EVENT_LISTENER_H
